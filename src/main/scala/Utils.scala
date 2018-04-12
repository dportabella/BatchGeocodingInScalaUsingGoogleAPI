import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.{KillSwitches, SharedKillSwitch}
import anorm.{Row, SimpleSql}
import com.typesafe.scalalogging.Logger

import scalaj.http.{Http, HttpOptions}

object Utils {
  def getDbConnection(dbUrl: String): Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    java.sql.DriverManager.getConnection(dbUrl)
  }

  def getDbConnection: Connection =
    getDbConnection(getProperty("dbUrl").get)

  def getProperty(name: String): Option[String] =
    Option(System.getProperty(name)).orElse(Option(System.getenv(name)))

  def executeOneRowUpdate(sql: SimpleSql[Row])(implicit conn: Connection) {
    val numUpdatedRows = sql.executeUpdate()
    if (numUpdatedRows != 1) throw new Exception(s"error saving to database. numUpdatedRows $numUpdatedRows != 1")
  }

  def textSample(text: Any): String =
    text.toString.replaceAll("\\s+", " ").take(300)

  def download(url: String): String =
    Http(url).option(HttpOptions.followRedirects(true)).asString.body

  def escapeSql(str: String): String =
    "\"" + str.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\""

  def escapeSql(str: Option[String]): String = str match {
    case Some(s) => escapeSql(s)
    case None => "NULL"
  }

  def sqlUpdateStmt(pairs: Seq[(String, Option[String])]): String =
    pairs.map { case (key, value) => key + " = " + escapeSql(value) }.mkString(", ")

  def retryFlow[T](maxErrors: Int, log: Logger): (SharedKillSwitch, Flow[Either[String, T], T, NotUsed]) = {
    val killSwitch = KillSwitches.shared("maxTries")
    val numErrorsAtomicInteger = new AtomicInteger(0)
    val countAndFilterErrors: Flow[Either[String, T], T, NotUsed] =
      Flow[Either[String, T]].map {
        case Right(r) => List(r)
        case Left(error) =>
          val numErrors = numErrorsAtomicInteger.getAndIncrement()
          log.error(s"numErrors #$numErrors errorsLeft=${maxErrors - numErrors}: $error!")
          if (numErrors >= maxErrors) {
            log.error(s"MaxErrors reached. Stopping {self.path.name}")
            killSwitch.shutdown()
          }
          List.empty
      }.mapConcat(identity)

    (killSwitch, countAndFilterErrors)
  }
}
