import DB.SaveGoogleResponse
import Utils.textSample
import BatchParserCmd.AvailableQuota
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString

object GoogleGeocoder {
  def props(googleApiKey: String, maxOpenRequests: Int, maxFatalErrors: Int, db: ActorRef, addressParser: ActorRef, parseAddress: Boolean, batchParserCmd: ActorRef): Props =
    Props(new GoogleGeocoder(googleApiKey, maxOpenRequests, maxFatalErrors, db, addressParser, parseAddress, batchParserCmd))

  final case class GeoCode(id: Int, unformattedAddress: String)
}

class GoogleGeocoder(googleApiKey: String, maxOpenRequests: Int, maxFatalErrors: Int, db: ActorRef, addressParser: ActorRef, parseAddress: Boolean, batchParserCmd: ActorRef) extends Actor with ActorLogging {
  import GoogleGeocoder._
  import AddressParserActor._
  import akka.pattern.pipe
  import context.dispatcher

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  val http = Http(context.system)

  var numFatalErrors = 0
  var numTotalRequests = 0
  val controlFactor = 10

  val queue = new scala.collection.mutable.Queue[(Int, String)]
  var numOpenRequests = 0

  var numRequests = 0

  def receive = {
    case GeoCode(id, unformattedAddress) =>
      numTotalRequests += 1
      if (numFatalErrors < maxFatalErrors) {
        log.info(s"GeoCode #$id: $unformattedAddress")
        if (numOpenRequests < maxOpenRequests)
          query(id, unformattedAddress)
        else
          queue += ((id, unformattedAddress))
      } else {
        log.info(s"GeoCode. ignored because of MaxFatalErrors")
      }
      sender ! AvailableQuota((maxOpenRequests - controlFactor * maxOpenRequests * (numFatalErrors / (numTotalRequests * 1.0)) - numOpenRequests).toInt)


    case (id: Int, resp @ HttpResponse(StatusCodes.OK, headers, entity, _)) =>
      log.info(s"Success response coming for #$id")
      entity.dataBytes.runFold(ByteString(""))(_ ++ _).map((id, _)).pipeTo(self)

    case (id: Int, body: ByteString) =>
      val googleResponse = body.utf8String
      numOpenRequests = numOpenRequests - 1
      queryNext()
      log.info(s"Success response for #$id: ${textSample(googleResponse)}")

      AddressParser.findGoogleGeocoderFatalErrorFromJsonResponse(googleResponse) match {
        case Some(googleGeocoderFatalError) => log.info(s"#$id: " + googleGeocoderFatalError.toString); fatalError()
        case None =>
          db ! SaveGoogleResponse(id, googleResponse)
          if (parseAddress)
            addressParser ! ParseAddress(id, googleResponse)
      }

    case (id: Int, resp @ HttpResponse(code, _, _, _)) =>
      log.info(s"Request failed for #$id, response code: $code")
      resp.discardEntityBytes()
      numOpenRequests = numOpenRequests - 1
      queryNext()
      fatalError()

    case f: Status.Failure =>
      log.info("failure" + textSample(f))

    case m =>
      log.info("unexpected message: " + textSample(m))
  }

  def query(id: Int, unformattedAddress: String) {
    if (numFatalErrors < maxFatalErrors) {
      numOpenRequests = numOpenRequests + 1
      numRequests = numRequests + 1
      log.info(s"query num $numRequests: #$id, $unformattedAddress")
      val url = AddressParser.url(googleApiKey, unformattedAddress)
      http
        .singleRequest(HttpRequest(uri = url))
        .map(r => (id, r))
        .pipeTo(self)
    } else {
      log.info(s"query. ignored because of MaxFatalErrors")
    }
  }

  def queryNext() {
    if (queue.nonEmpty) {
      val (id: Int, unformattedAddress: String) = queue.dequeue
      query(id, unformattedAddress)
    }
    batchParserCmd ! AvailableQuota((maxOpenRequests - controlFactor * maxOpenRequests * (numFatalErrors / (numTotalRequests * 1.0)) - numOpenRequests).toInt)
  }

  def fatalError() {  // TODO: I get several fatalError #0. not thead-safe?!
    log.info(s"fatalError #$numFatalErrors on ${self.path.name}")
    numFatalErrors = numFatalErrors + 1
    if (numFatalErrors > maxFatalErrors) {
      log.info(s"MaxFatalErrors reached. stopping ${self.path.name}")
      context.stop(self)
    }
  }
}
