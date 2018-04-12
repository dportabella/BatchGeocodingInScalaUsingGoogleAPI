import Model._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

object BatchParserCmd {
  private val log = Logger("BatchParserCmd")

  case class Config(
                     op: String = "",
                     maxEntries: Int = 100,
                     maxGoogleAPIRequestsPerSecond: Int = 10,
                     maxGoogleAPIFatalErrors: Int = 5,
                     googleApiKey: String = "",
                     dbUrl: String = "",
                     tableName: String = ""
                   )

  val parser = new scopt.OptionParser[Config]("BatchParserCmd") {
    override def showUsageOnError = true

    opt[String]("op").required.action((x, c) =>
      c.copy(op = x)).text("googleQueryAndParse, googleQueryOnly or parseOnly")

    opt[Int]("maxEntries").required.action((x, c) =>
      c.copy(maxEntries = x)).text("maxEntries")

    opt[Int]("maxGoogleAPIRequestsPerSecond").optional.action((x, c) =>
      c.copy(maxGoogleAPIRequestsPerSecond = x)).text("maxGoogleAPIRequestsPerSecond")

    opt[Int]("maxGoogleAPIFatalErrors").optional.action((x, c) =>
      c.copy(maxGoogleAPIFatalErrors = x)).text("maxGoogleAPIFatalErrors")

    opt[String]("googleApiKey").optional.action((x, c) =>
      c.copy(googleApiKey = x)).text("googleApiKey")

    opt[String]("dbUrl").action((x, c) =>
      c.copy(dbUrl = x)).text("dbUrl")

    opt[String]("tableName").action((x, c) =>
      c.copy(tableName = x)).text("tableName")

    version("version")
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        log.info("+++ config: " + config)

        require(config.op == "googleQueryAndParse" || config.op == "googleQueryOnly" || config.op == "parseOnly")

        implicit val system: ActorSystem = ActorSystem()
        implicit val materializer: Materializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher

        val db = new DB(config.dbUrl, config.tableName)

        def terminateSystem() = {
          log.info("terminating actor system")
          Http().shutdownAllConnectionPools()
          system.terminate()
        }

        if (config.op == "googleQueryAndParse" || config.op == "googleQueryOnly") {
          val parseAddress = config.op == "googleQueryAndParse"
          googleQueryAndParse(parseAddress, config.maxEntries, config.googleApiKey, config.maxGoogleAPIRequestsPerSecond, config.maxGoogleAPIFatalErrors, db, terminateSystem)
        } else {
          parseOnly(system, config.maxEntries, db, terminateSystem)
        }
      case None => sys.exit(1)
    }
  }

  def googleQueryAndParse(
                           parseAddress: Boolean,
                           maxEntries: Int,
                           googleApiKey: String,
                           maxGoogleAPIRequestsPerSecond: Int,
                           maxGoogleAPIFatalErrors: Int,
                           db: DB,
                           finishedFn: () => Unit
                         )
                         (implicit actorSystem: ActorSystem, materialize: Materializer, executionContent: ExecutionContext) {

    val googleApiResponse: Source[GoogleResponse, NotUsed] =
      GoogleGeocoder.flow(db, googleApiKey, maxGoogleAPIRequestsPerSecond, maxGoogleAPIFatalErrors, maxEntries)

    if(!parseAddress){
      googleApiResponse
        .runWith(db.saveGoogleResponse())
        .foreach(_ => finishedFn())
    } else {
      googleApiResponse
        .map(parse)
        .runWith(db.saveAddressParsingResultSink())
        .recover { case t: Throwable => log.error("googleQueryAndParse error", t); Done }
        .foreach(_ => finishedFn())
    }
  }

  def parseOnly(
                 system: ActorSystem,
                 maxEntries: Int,
                 db: DB,
                 finishedFn: () => Unit
               )
               (implicit actorSystem: ActorSystem, materialize: Materializer, executionContent: ExecutionContext) {
    db.getUnparsedGoogleResponsesFromDatabase(maxEntries)
      .map(parse)
      .runWith(db.saveAddressParsingResultSink())
      .recover { case t: Throwable => log.error("parseOnly error", t); Done }
      .foreach(_ => finishedFn())
  }

  def parse(googleResponse: GoogleResponse): AddressParsingResult = {
    val result = Try(AddressParser.parseAddressFromJsonResponse(googleResponse.googleResponse))
    AddressParsingResult(googleResponse, result)
  }
}
