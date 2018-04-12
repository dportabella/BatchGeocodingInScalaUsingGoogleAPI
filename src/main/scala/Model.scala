import AddressParser.ParsedAddress

import scala.util.Try

object Model {
	case class UnformattedAddress(id: Int, unformattedAddress: String)
	case class GoogleResponse(id: Int, googleResponse: String)
	case class AddressParsingResult(googleApiResponse: GoogleResponse, parsedAddress: Try[Option[ParsedAddress]])
}
