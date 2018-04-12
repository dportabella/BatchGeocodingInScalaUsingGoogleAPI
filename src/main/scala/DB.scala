import java.security.InvalidParameterException

import Model._
import Utils._
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl.{Source, _}
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DB {
  def createTableStmt(tableName: String, addressLength: Int = 500, addressComponentsLength: Int = 200, maxLongTextIndexLength: Int = 100, unformattedAddressUnique: Boolean = true, maxIndexLength: Option[Int] = None): String = {
    if (unformattedAddressUnique && maxIndexLength.isDefined) throw new InvalidParameterException("unformattedAddressUnique && maxIndexLength.isDefined")
    val ls = maxIndexLength.map(l => s"($l)").getOrElse("")
    val unique = if (unformattedAddressUnique) "unique" else ""
    s"""
       |create table $tableName (
       |  id int not null auto_increment primary key,
       |  unformattedAddress varchar($addressLength) not null,
       |  ts timestamp default current_timestamp on update current_timestamp,
       |  googleResponse longtext,
       |  parseGoogleResponseStatus longtext,
       |  numResults int,
       |  formattedAddress varchar($addressLength),
       |  lat float(10,6), lng float(10,6), mainType varchar($addressComponentsLength), types longtext, viewportArea float,
       |  ${AddressParser.addressComponentTypes.map(c => s"$c varchar($addressComponentsLength)").mkString(", ")},
       |  $unique index(unformattedAddress$ls), index(ts), index(googleResponse($maxLongTextIndexLength)), index(parseGoogleResponseStatus($maxLongTextIndexLength)), index(numResults), index(formattedAddress$ls),
       |  index(lat), index(lng), index(mainType$ls), index(types($maxLongTextIndexLength)), index(viewportArea),
       |  ${AddressParser.addressComponentTypes.map(c => s"index($c$ls)").mkString(", ")}
       |) engine = InnoDB default character set = utf8mb4 collate = utf8mb4_bin row_format=dynamic
    """.stripMargin
  }

  val addressComponentTypesUpdateStmt: String =
    AddressParser.addressComponentTypes.map(t => s"$t = {$t}").mkString(", ")

  val addressComponentTypesNullUpdateStmt: String =
    AddressParser.addressComponentTypes.map(t => s"$t = null").mkString(", ")
}

class DB(dbUrl:String, tableName: String)(implicit actorSystem: ActorSystem) {
  import DB._

  private val log = Logger("DB")

  private val slickDbConfig =
    ConfigFactory.parseString(s"""db.properties.url = "$dbUrl"""")
      .withFallback(ConfigFactory.load().getConfig("slick-database"))

  private implicit val slickSession: SlickSession = SlickSession.forConfig(slickDbConfig)
  actorSystem.registerOnTermination(() => slickSession.close())
  import slickSession.profile.api._

  def getAddressesWithEmptyGoogleResponseFromDatabase(maxEntries: Int)(implicit materializer: Materializer): Source[UnformattedAddress, NotUsed] = Slick.source(
    sql"select id, unformattedAddress from #$tableName where googleResponse is null limit $maxEntries"
      .as(GetResult(r => UnformattedAddress(r.nextInt, r.nextString)))
  )

  def getUnparsedGoogleResponsesFromDatabase(maxEntries: Int)(implicit materializer: Materializer): Source[GoogleResponse, NotUsed] = Slick.source(
    sql"select id, googleResponse from #$tableName where googleResponse is not null and parseGoogleResponseStatus is null limit $maxEntries"
      .as(GetResult(r => GoogleResponse(r.nextInt, r.nextString)))
  )

  def saveGoogleResponse(): Sink[GoogleResponse, Future[Done]] = Slick.sink {
    case GoogleResponse(id, googleResponse) =>
      log.info(s"SaveGoogleResponse for #$id: ${textSample(googleResponse)}")
      // todo: this might overwrite the SaveGoogleResponseAndAddress
      sqlu"update #$tableName set googleResponse=$googleResponse, parseGoogleResponseStatus=null, numResults=null, #${addressComponentTypesNullUpdateStmt}, lat=null, lng=null, mainType=null, types=null, viewportArea=null, formattedAddress=null where id=$id"
  }

  def saveAddressParsingResultSink()(implicit executionContent: ExecutionContext): Sink[AddressParsingResult, Future[Done]] = Slick.sink {
    case AddressParsingResult(GoogleResponse(id, googleResponse), Success(Some(parsedAddress))) =>
      log.info(s"SaveGoogleResponseAndAddress for #$id: ${textSample(googleResponse)}, ${textSample(parsedAddress.toString)}")
      import parsedAddress._

      val addressComponentTypesUpdateStmt =
        sqlUpdateStmt(AddressParser.addressComponentTypes.map(t => (t, parsedAddress.addressComponents.get(t))))

      sqlu"update #$tableName set googleResponse=$googleResponse, parseGoogleResponseStatus='OK', numResults=$numResults, #$addressComponentTypesUpdateStmt, lat=${location.map(_.lat)}, lng=${location.map(_.lng)}, mainType=$mainType, types=${types.mkString(", ")}, viewportArea=$viewportArea, formattedAddress=$formattedAddress where id=$id"

    case AddressParsingResult(GoogleResponse(id, googleResponse), Success(None)) =>
      log.info(s"SaveGoogleResponseAndEmptyResult for #$id: ${textSample(googleResponse)}")
      sqlu"update #$tableName set googleResponse=$googleResponse, parseGoogleResponseStatus='OK', numResults=0, #$addressComponentTypesNullUpdateStmt, lat=null, lng=null, mainType=null, types=null, viewportArea=null, formattedAddress=null where id=$id"

    case AddressParsingResult(GoogleResponse(id, googleResponse), Failure(exception)) =>
      log.info(s"SaveError: $id, $exception")
      sqlu"update #$tableName set parseGoogleResponseStatus=${exception.getMessage} where id=$id"
  }
}
