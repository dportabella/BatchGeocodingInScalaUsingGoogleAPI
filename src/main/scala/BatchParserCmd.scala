
import akka.actor._

import scala.io.StdIn

object BatchParserCmd {
  case class Config(
                   op: String = "",
                   maxEntries: Int = 100,
                   maxGoogleAPIOpenRequests: Int = 10,
                   maxGoogleAPIFatalErrors: Int = 5,
                     googleApiKey: String = "",
                     dbUrl: String = "",
                     tableName: String = ""
                   )

  case class AvailableQuota(availableRequests: Int)
  case object StartParsing

  val parser = new scopt.OptionParser[Config]("BatchParserCmd") {
    override def showUsageOnError = true

    opt[String]("op").required.action((x, c) =>
      c.copy(op = x)).text("googleQueryAndParse, googleQueryOnly or parseOnly")

    opt[Int]("maxEntries").required.action((x, c) =>
      c.copy(maxEntries = x)).text("maxEntries")

    opt[Int]("maxGoogleAPIOpenRequests").optional.action((x, c) =>
      c.copy(maxGoogleAPIOpenRequests = x)).text("maxGoogleAPIOpenRequests")

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

  var system: ActorSystem = null
  var db: ActorRef = null
  var addressParser: ActorRef = null
  var googleGeocoder: ActorRef = null
  var batchParserCmd: ActorRef = null

  var tempListAddresses: List[(Int, String)] = null

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        println("+++ config: " + config)

        require(config.op == "googleQueryAndParse" || config.op == "googleQueryOnly" || config.op == "parseOnly")

        val parseAddress = config.op == "googleQueryAndParse"
        setupActorSystem(config.googleApiKey, config.maxGoogleAPIOpenRequests, config.maxGoogleAPIFatalErrors, parseAddress, config.dbUrl, config.tableName)
        try {
          if (config.op == "googleQueryAndParse" || config.op == "googleQueryOnly") {

            googleQueryAndParse(config.maxEntries, config.dbUrl, config.tableName)
          } else {
            parseOnly(system, config.maxEntries, config.dbUrl, config.tableName)
          }
          println(">>> Press ENTER to exit <<<")
          StdIn.readLine()
        } finally {
          println("Terminating Actor System")
          system.terminate()
        }
      case None => sys.exit(1)
    }
  }

  def setupActorSystem(googleApiKey: String, maxOpenRequests: Int, maxFatalErrors: Int, parseAddress: Boolean, dbUrl: String, tableName: String): Unit = {
    system = ActorSystem("System")
    db = system.actorOf(DB.props(dbUrl, tableName), "DB")
    batchParserCmd = system.actorOf(BatchParserCmd.props(), "BatchParserCmd")
    addressParser = system.actorOf(AddressParserActor.props(db), "AddressParser")
    googleGeocoder = system.actorOf(GoogleGeocoder.props(googleApiKey, maxOpenRequests: Int, maxFatalErrors: Int, db, addressParser, parseAddress, batchParserCmd), "GoogleAPI")
  }

  def googleQueryAndParse(maxEntries: Int, dbUrl: String, tableName: String) {
    val conn = Utils.getDbConnection(dbUrl)
    val unformattedAddresses: List[(Int, String)] = try {
      DB.getAddressesWithEmptyGoogleResponseFromDatabase(tableName, maxEntries)(conn)
    } finally { conn.close() }

    println(s"num unformattedAddresses to query: ${unformattedAddresses.length}")

    tempListAddresses = unformattedAddresses
    batchParserCmd ! StartParsing
  }

  def parseOnly(system: ActorSystem, maxEntries: Int, dbUrl: String, tableName: String) {
    val conn = Utils.getDbConnection(dbUrl)
    val googleResponses: List[(Int, String)] = try {
      DB.getUnparsedGoogleResponsesFromDatabase(tableName, maxEntries)(conn)
    } finally { conn.close() }

    println(s"num googleResponses: ${googleResponses.length}")

    googleResponses.foreach { case (id, googleResponse) => addressParser ! AddressParserActor.ParseAddress(id, googleResponse) }
  }

  def props(): Props =
    Props(new BatchParserCmd())
}

case class BatchParserCmd() extends Actor with ActorLogging {
  import BatchParserCmd._

  def receive = {
    case StartParsing => {
      sendForParsing(1)
    }

    case AvailableQuota(availableRequests) => {
      if (availableRequests <= 0) {
        log.info(s"Available quota is $availableRequests")
      } else {
        sendForParsing(availableRequests)
      }
    }

    case m => log.info("unexpected message: " + Utils.textSample(m))
  }

  def sendForParsing(batchSize: Int): Unit = {
    if (tempListAddresses == null || tempListAddresses.isEmpty) {
      return ;
    }
    tempListAddresses.slice(0, batchSize + 1).foreach {
      case (id, unformattedAddresses) => googleGeocoder ! GoogleGeocoder.GeoCode(id, unformattedAddresses)
    }
    tempListAddresses = tempListAddresses.slice(batchSize, tempListAddresses.size)
  }
}
