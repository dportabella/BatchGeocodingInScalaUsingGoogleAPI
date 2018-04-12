import java.util.concurrent.TimeoutException

import Model._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object GoogleGeocoder {
  private val log = Logger("GoogleGeocoder")

  def flow(db: DB,
           googleApiKey: String,
           maxRequestsPerSecond: Int,
           maxFatalErrors: Int,
           maxEntries: Int)
          (implicit
                actorSystem: ActorSystem,
                materialize: Materializer,
                executionContent: ExecutionContext): Source[GoogleResponse, NotUsed] = {

    val http = Http(actorSystem)

    val queryGoogleApiFlow: Flow[UnformattedAddress, Either[String, GoogleResponse], _] = Flow[UnformattedAddress].mapAsync(maxRequestsPerSecond) { geoCode: UnformattedAddress =>
      val uri = AddressParser.url(googleApiKey, geoCode.unformattedAddress)

      http.singleRequest(request = HttpRequest(uri = uri)).flatMap {
        case resp @ HttpResponse(StatusCodes.OK, headers, entity, protocol) =>
          entity.dataBytes.runFold(ByteString(""))(_ ++ _)
            .map(_.utf8String)
            .map(r => validGoogleResponse(geoCode.id, r))
        case resp @ HttpResponse(code, headers, entity, protocol) =>
          resp.discardEntityBytes()
          val msg = s"Request failed for #${geoCode.id}, response code: $code"
          log.error(msg)
          Future.successful(Left(msg))
      }.recover {
        case error: akka.stream.StreamTcpException =>
          val msg = s"Request failed for #${geoCode.id}: ${error.getMessage}"
          log.error(msg)
          throw new TimeoutException(msg)
        case error =>
          val msg = s"Request failed for #${geoCode.id}: ${error.getMessage}"
          log.error(msg)
          Left(msg)
      }
    }

    val googleApiResponse: Source[Either[String, GoogleResponse], NotUsed] =
      db.getAddressesWithEmptyGoogleResponseFromDatabase(maxEntries)
        .throttle(maxRequestsPerSecond, 1.second, 0, ThrottleMode.Shaping)
        .via(queryGoogleApiFlow)
        .mapMaterializedValue(_ => NotUsed)

    val (killSwitch, countAndFilterFatalErrors) = Utils.retryFlow[GoogleResponse](maxFatalErrors, log)

    googleApiResponse
      .recoverWithRetries(-1, { case _ => googleApiResponse })
      .via(killSwitch.flow)
      .via(countAndFilterFatalErrors)
  }

  def validGoogleResponse(geocodeId: Int, googleResponse: String): Either[String, GoogleResponse] =
    AddressParser.findGoogleGeocoderFatalErrorFromJsonResponse(googleResponse) match {
      case Some(error) => Left(error.getMessage)
      case None => Right(GoogleResponse(geocodeId, googleResponse))
    }
}
