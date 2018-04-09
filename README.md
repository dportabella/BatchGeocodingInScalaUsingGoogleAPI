# BatchGeocodingInScalaUsingGoogleAPI

This is a simple Scala program for parsing a list of addresses using google maps api.

The main function for parsing a single address is implemented on `src/main/scala/AddressParser.scala`:
- It builds the proper URL google maps query with the requested address
- It uses Play Json to parse the json response
- It extracts the data we need and it builds an Address case class

`BatchParsderCmd` uses akka in order to query and parse a list of addresses asynchronously.
It queries the addresses from a database, and it uses three actors: GoogleGeocoder, AddressParserActor, and DB to save the results.


# Requirements
You need to install:
- [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/)
- [SBT](http://www.scala-sbt.org/) (build tool for Scala)
- Google Maps Geocoding API key. Get your free account [here](https://developers.google.com/maps/documentation/geocoding/start#get-a-key).
  It will be something like `AIzaSyBwG-Zo6me1yd6V2_ZO7L-3K8A0U1122LA`
- Optional: [IntelliJ](https://www.jetbrains.com/idea/) IDE with the Scala plugin


# Example: parse one address
```
$ export googleApiKey="AIzaSyBwG-Zo6me1yd6V2_ZO7L-3K8A0U1122LA"   # update with your valid api key
$ sbt "runMain ParserCmd $googleApiKey \"Av du Rond-Point 1, Lausanne, Switzerland\""

- parsedAddress:
| - numResults: 2
| - addressComponents:                  # only are take into account the ones defined on src/main/scala/AddressParser.scala -> addressComponentTypes 
| | - country: United States
| | - administrative_area_level_1: West Virginia
| | - administrative_area_level_2: Putnam County
| | - postal_code: 25109
| | - route: 1st Avenue North
| | - locality: Hometown
| | - administrative_area_level_3: Buffalo-Union
| - location:
| | - lat: 38.535866 
| | - lng: -81.86641
| - formattedAddress: 1st Ave N, Hometown, WV 25109, USA
| - mainType: route                     # this is computed according to src/main/scala/AddressParser.scala -> mainTypeOrder
| - types: [route]
| - viewportArea: 361119.3987508803     # this is computed from result.geometry.viewport
```

# How it works
Given this example address: "Av du Rond-Point 1, Lausanne, Switzerland"

## url
We construct this google query url (replace your valid api key):
```
https://maps.googleapis.com/maps/api/geocode/json?address=Av%20du%20Rond-Point%201,%20Lausanne,%20Switzerland&key=AIzaSyBwG-Zo6me1yd6V2_ZO7L-3K8A0U1122LA
```

## json response
We get this JSON response:
```
{
   "results" : [
      {
         "address_components" : [
            {
               "long_name" : "81",
               "short_name" : "81",
               "types" : [ "street_number" ]
            },
            {
               "long_name" : "Rue de Genève",
               "short_name" : "Rue de Genève",
               "types" : [ "route" ]
            },
            {
               "long_name" : "Lausanne",
               "short_name" : "Lausanne",
               "types" : [ "locality", "political" ]
            },
            {
               "long_name" : "Lausanne",
               "short_name" : "Lausanne",
               "types" : [ "administrative_area_level_2", "political" ]
            },
            {
               "long_name" : "Vaud",
               "short_name" : "VD",
               "types" : [ "administrative_area_level_1", "political" ]
            },
            {
               "long_name" : "Switzerland",
               "short_name" : "CH",
               "types" : [ "country", "political" ]
            },
            {
               "long_name" : "1004",
               "short_name" : "1004",
               "types" : [ "postal_code" ]
            }
         ],
         "formatted_address" : "Rue de Genève 81, 1004 Lausanne, Switzerland",
         "geometry" : {
            "bounds" : {
               "northeast" : {
                  "lat" : 46.5249294,
                  "lng" : 6.613929199999999
               },
               "southwest" : {
                  "lat" : 46.52491699999999,
                  "lng" : 6.613923799999999
               }
            },
            "location" : {
               "lat" : 46.5249294,
               "lng" : 6.613929199999999
            },
            "location_type" : "RANGE_INTERPOLATED",
            "viewport" : {
               "northeast" : {
                  "lat" : 46.52627218029149,
                  "lng" : 6.615275480291501
               },
               "southwest" : {
                  "lat" : 46.5235742197085,
                  "lng" : 6.612577519708497
               }
            }
         },
         "place_id" : "EihSdWUgZGUgR2Vuw6h2ZSA4MSwgMTAwNCBMYXVzYW5uZSwgU3Vpc3Nl",
         "types" : [ "street_address" ]
      }
   ],
   "status" : "OK"
}
```

## `src/main/scala/AddressParser.scala`
It parses the json response using the play json library.
It uses the implicit formats, which use the name of the case class fields to extract the data from the json response, and it ignores the rest of the json response.
Then it extracts the data of interest (for instance, it finds the `address_component` with at least one type "administrative_area_level_1", and it maps it to `areaLevel1`).
It builds an `Address` case class (defined also at `AddressParser.scala`) as shown in the previous "Example: parse one address" section.

This was enough for our use case, but it is easy to modify `AddressParser.scala` for extracting more info from the json response.


# Batch geocoding
We have a sql database, with a list of addresses to query and a place to save the extracted information.
We have a table called `addresses`, and it contains these fields:
```
- unformattedAddress
- googleResponse
- parseGoogleResponseStatus
- numResults
- locality
- administrative_area_level_1
- administrative_area_level_2
...
```

## create the database
We can create the table as follows:
```
mysql>
create database test default character set utf8mb4 collate utf8mb4_unicode_ci;
use test
drop table if exists addresses;
create table addresses (
  id int not null auto_increment primary key,
  unformattedAddress varchar(500) not null,
  ts timestamp default current_timestamp on update current_timestamp,
  googleResponse longtext,
  parseGoogleResponseStatus longtext,
  numResults int,
  formattedAddress varchar(500),
  lat float(10,6), lng float(10,6), mainType varchar(200), types longtext, viewportArea float,
  administrative_area_level_1 varchar(200), administrative_area_level_2 varchar(200), administrative_area_level_3 varchar(200), administrative_area_level_4 varchar(200), administrative_area_level_5 varchar(200), airport varchar(200), country varchar(200), establishment varchar(200), floor varchar(200), locality varchar(200), natural_feature varchar(200), neighborhood varchar(200), park varchar(200), point_of_interest varchar(200), post_box varchar(200), postal_code varchar(200), postal_code_prefix varchar(200), postal_code_suffix varchar(200), postal_town varchar(200), premise varchar(200), route varchar(200), street_address varchar(200), street_number varchar(200), sublocality varchar(200), sublocality_level_1 varchar(200), sublocality_level_2 varchar(200), sublocality_level_3 varchar(200), sublocality_level_4 varchar(200), sublocality_level_5 varchar(200), subpremise varchar(200), ward varchar(200),
  unique index(unformattedAddress), index(ts), index(googleResponse(100)), index(parseGoogleResponseStatus(100)), index(numResults), index(formattedAddress),
  index(lat), index(lng), index(mainType), index(types(100)), index(viewportArea),
  index(administrative_area_level_1), index(administrative_area_level_2), index(administrative_area_level_3), index(administrative_area_level_4), index(administrative_area_level_5), index(airport), index(country), index(establishment), index(floor), index(locality), index(natural_feature), index(neighborhood), index(park), index(point_of_interest), index(post_box), index(postal_code), index(postal_code_prefix), index(postal_code_suffix), index(postal_town), index(premise), index(route), index(street_address), index(street_number), index(sublocality), index(sublocality_level_1), index(sublocality_level_2), index(sublocality_level_3), index(sublocality_level_4), index(sublocality_level_5), index(subpremise), index(ward)
) engine = InnoDB default character set = utf8mb4 collate = utf8mb4_bin row_format=dynamic;
```

Note: we use `float(10,6)` for storing lat and lng, as proposed by the [google api example](https://developers.google.com/maps/solutions/store-locator/clothing-store-locator?csw=1).
Note: if you get an `ERROR 1709 (HY000): Index column size too large. The maximum column size is 767 bytes.`, make sure you added `row_format=dynamic` at the end of the query, or configure correctly mysql server for `row_format dynamic`, or update your mysql server to the latest version.
Note: if you get an `ERROR 1713 (HY000): Undo log record is too big.`, try updating the table with `alter table addresses row_format=redundant;`
Note: read about `collate = utf8mb4_bin vs collate = utf8mb4_unicode_ci`;

We can insert the addresses to query as follows:
```
mysql> insert into addresses_todelete1 (unformattedAddress) values 
  ('Av du Rond-Point 1, Lausanne, Switzerland'),
  ('Statue, Palayam, Thiruvananthapuram, Kerala 695001, India'),
  ('Frankfurt am Main 80, DE'),
  ('(GSF);;INGOLSTAEDTER LANDSTR. 1;;D-8042 NEUHERBERG, DE'),
  ('CAREL VAN BYLANDTLAAN 30, LA HAYA NL, NL');
```
or from another table/query, such as from patstat:
```
mysql> insert ignore into addresses (unformattedAddress) select distinct(trim(concat(address_freeform, ', ', person_ctry_code))) from tls226_person_orig where nullif(address_freeform, '') is not null;
```


## Run the program
### Database connection details
To connect to the database, we need to specify a JDBC URL. It looks as follows, depending on whether it is mysql, postgresql or something else:
```
$ export dbUrl="jdbc:mysql://SERVER_HOST/DATABASE?user=USER&password=PASSWORD&useSSL=false&useUnicode=yes&characterEncoding=utf8"
$ export dbUrl="jdbc:postgresql://SERVER_HOST/DATABASE?user=USER&password=PASSWORD&ssl=true"
```

### Run
```
$ export googleApiKey="AIzaSyBwG-Zo6me1yd6V2_ZO7L-3K8A0U1122LA"   # update with your valid api key

$ sbt "runMain BatchParserCmd --help"
Usage: BatchParserCmd [options]

  --op <value>    where value = googleQueryAndParse, googleQueryOnly or parseOnly
  --maxEntries <value>
  --maxGoogleAPIOpenRequests <value>
  --maxGoogleAPIFatalErrors <value>
  --googleApiKey <value>
  --dbUrl <value>
  --tableName <value>
  --version                

$ sbt "runMain BatchParserCmd --op=googleQueryAndParse --maxEntries=20 --maxGoogleAPIOpenRequests=10 --maxGoogleAPIFatalErrors=5 --googleApiKey="$googleApiKey" --dbUrl="$dbUrl" --tableName=addresses"
```
`maxGoogleQueries` is the max number of google queries to do. It's best to try with a small number first.
The program will also stop if the max number of queries to the google api is exceeded (2500 request per day for the free account).
You can then execute this same command the following day, and it will resume the process (it will not re-download what it has queried already). 

This version of the program fails with akka showing "dead letters" if there are too many addresses.
A workaround is to call this command before running sbt: `export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xss2M  -Duser.timezone=GMT"`.
The better option is to use the akka stream version of this program, which uses back-pressure. See the git branch of this project.

The program queries google in parallel. The bigger `maxOpenRequests`, the faster to query all addresses. Google has a rate limit, so try before with smalls numbers. Not more than 32.

The program will stop after `maxFatalErrors`. Set a small number.


### Query the results
```
mysql>
  set @database = 'david'; set @table = 'addresses';
  set @sql = concat('create view ', @table, '_ as select ', (select replace(group_concat(column_name), 'googleResponse,', "concat(left(replace(googleResponse, '\n', ' '), 20), '...') as googleResponseCrop,") from information_schema.columns where table_name = @table and table_schema = @database), ' from ', @table);
  prepare stmt from @sql; execute stmt;

mysql> select * from addresses_;                                                                                                                                                                                                                                        
mysql> select * from addresses_todelete1_;
+----+-----------------------------------------------------------+---------------------+-------------------------+---------------------------+------------+-----------------------------------------------------------+-----------+------------+---------------------+---------------------------------------------+--------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+---------+---------------+---------------+-------+--------------------+-----------------+--------------+------+-------------------+----------+-------------+--------------------+--------------------+-------------+---------+-----------------------+----------------+---------------+-------------+---------------------+---------------------+---------------------+---------------------+---------------------+------------+------+
| id | unformattedAddress                                        | ts                  | googleResponseCrop      | parseGoogleResponseStatus | numResults | formattedAddress                                          | lat       | lng        | mainType            | types                                       | viewportArea | administrative_area_level_1 | administrative_area_level_2 | administrative_area_level_3 | administrative_area_level_4 | administrative_area_level_5 | airport | country       | establishment | floor | locality           | natural_feature | neighborhood | park | point_of_interest | post_box | postal_code | postal_code_prefix | postal_code_suffix | postal_town | premise | route                 | street_address | street_number | sublocality | sublocality_level_1 | sublocality_level_2 | sublocality_level_3 | sublocality_level_4 | sublocality_level_5 | subpremise | ward |
+----+-----------------------------------------------------------+---------------------+-------------------------+---------------------------+------------+-----------------------------------------------------------+-----------+------------+---------------------+---------------------------------------------+--------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-----------------------------+---------+---------------+---------------+-------+--------------------+-----------------+--------------+------+-------------------+----------+-------------+--------------------+--------------------+-------------+---------+-----------------------+----------------+---------------+-------------+---------------------+---------------------+---------------------+---------------------+---------------------+------------+------+
| 41 | Av du Rond-Point 1, Lausanne, Switzerland                 | 2017-11-20 18:02:08 | {    "results" : [  ... | OK                        |          1 | Avenue du Rond-Point 1, 1006 Lausanne, Switzerland        | 46.515224 |   6.628633 | point_of_interest   | establishment, point_of_interest, premise   |      61911.9 | Vaud                        | Lausanne                    | NULL                        | NULL                        | NULL                        | NULL    | Switzerland   | NULL          | NULL  | Lausanne           | NULL            | NULL         | NULL | NULL              | NULL     | 1006        | NULL               | NULL               | NULL        | NULL    | Avenue du Rond-Point  | NULL           | 1             | NULL        | NULL                | NULL                | NULL                | NULL                | NULL                | NULL       | NULL |
| 42 | Statue, Palayam, Thiruvananthapuram, Kerala 695001, India | 2017-11-20 18:02:08 | {    "results" : [  ... | OK                        |          1 | Statue, Palayam, Thiruvananthapuram, Kerala 695001, India |  8.496704 |  76.950623 | sublocality_level_2 | political, sublocality, sublocality_level_2 |       386070 | Kerala                      | Thiruvananthapuram          | NULL                        | NULL                        | NULL                        | NULL    | India         | NULL          | NULL  | Thiruvananthapuram | NULL            | NULL         | NULL | NULL              | NULL     | 695001      | NULL               | NULL               | NULL        | NULL    | NULL                  | NULL           | NULL          | Statue      | Palayam             | Statue              | NULL                | NULL                | NULL                | NULL       | NULL |
| 43 | Frankfurt am Main 80, DE                                  | 2017-11-20 18:02:08 | {    "results" : [  ... | OK                        |          3 | 80 Main Dr, Frankfort, IN 46041, USA                      | 40.296558 | -86.513023 | street_address      | street_address                              |      68497.2 | Indiana                     | Clinton County              | Center Township             | NULL                        | NULL                        | NULL    | United States | NULL          | NULL  | Frankfort          | NULL            | NULL         | NULL | NULL              | NULL     | 46041       | NULL               | NULL               | NULL        | NULL    | Main Drive            | NULL           | 80            | NULL        | NULL                | NULL                | NULL                | NULL                | NULL                | NULL       | NULL |
| 44 | (GSF);;INGOLSTAEDTER LANDSTR. 1;;D-8042 NEUHERBERG, DE    | 2017-11-20 18:02:08 | {    "results" : [],... | OK                        |          0 | NULL                                                      |      NULL |       NULL | NULL                | NULL                                        |         NULL | NULL                        | NULL                        | NULL                        | NULL                        | NULL                        | NULL    | NULL          | NULL          | NULL  | NULL               | NULL            | NULL         | NULL | NULL              | NULL     | NULL        | NULL               | NULL               | NULL        | NULL    | NULL                  | NULL           | NULL          | NULL        | NULL                | NULL                | NULL                | NULL                | NULL                | NULL       | NULL |
| 45 | CAREL VAN BYLANDTLAAN 30, LA HAYA NL, NL                  | 2017-11-20 18:02:08 | NULL                    | NULL                      |       NULL | NULL                                                      |      NULL |       NULL | NULL                | NULL                                        |         NULL | NULL                        | NULL                        | NULL                        | NULL                        | NULL                        | NULL    | NULL          | NULL          | NULL  | NULL               | NULL            | NULL         | NULL | NULL              | NULL     | NULL        | NULL               | NULL               | NULL        | NULL    | NULL                  | NULL           | NULL          | NULL        | NULL                | NULL                | NULL                | NULL                | NULL                | NULL       | NULL |
...
```


### See stats
This is an example results after querying 250 addresses:
```
$ sbt "runMain BatchParserCmd --op=googleQueryAndParse --maxEntries=250 --googleApiKey="$googleApiKey" --dbUrl="$dbUrl" --tableName=addresses"

mysql> select count(*), googleResponse is not null as googleResponseStored, parseGoogleResponseStatus, numResults from addresses group by googleResponseStored, parseGoogleResponseStatus, numResults;
+----------+----------------------+-----------------------------------+------------+
| count(*) | googleResponseStored | parseGoogleResponseStatus         | numResults |
+----------+----------------------+-----------------------------------+------------+
|   860838 |                    0 | NULL                              |       NULL |
|       58 |                    1 | OK                                |          0 |
|       13 |                    1 | OK                                |          2 |
|      179 |                    1 | OK                                |          1 |
+----------+----------------------+-----------------------------------+------------+
4 rows in set (3.37 sec)
```

It says that:
- 179 cases are perfect (queried and parsed successfully).
- 13 cases, where google provided more than one result because the address is ambiguous (we take only the first result) 
- 58 cases, where google did not find any result
- 860'838 cases that we still didn't query.

We can resume the program and query 100 more addresses:
```
$ sbt "runMain BatchParserCmd --op=googleQueryAndParse --maxEntries=100 --googleApiKey="$googleApiKey" --dbUrl="$dbUrl" --tableName=addresses"

mysql> select count(*), googleResponse is not null as googleResponseStored, parseGoogleResponseStatus, numResults from addresses group by googleResponseStored, parseGoogleResponseStatus, numResults;
+----------+----------------------+-----------------------------------+------------+
| count(*) | googleResponseStored | parseGoogleResponseStatus         | numResults |
+----------+----------------------+-----------------------------------+------------+
|   860738 |                    0 | NULL                              |       NULL |
|       68 |                    1 | OK                                |          0 |
|       19 |                    1 | OK                                |          2 |
|      263 |                    1 | OK                                |          1 |
+----------+----------------------+-----------------------------------+------------+
4 rows in set (3.67 sec)
```

and so on...

# Re-excuting the parser without re-querying google
We keep a copy of the google responses in the database on the column `googleResponse`.
We do not need to re-query google in the case we change the code of `AddressParser` (to fix an issue, or to extract more data from the json response).

```
mysql> update addresses set parseGoogleResponseStatus = null, numResults = null;
Query OK, 274 rows affected (2.80 sec)
Rows matched: 861088  Changed: 274  Warnings: 0

mysql> select count(*), googleResponse is not null as googleResponseStored, parseGoogleResponseStatus, numResults from addresses group by googleResponseStored, parseGoogleResponseStatus, numResults;
+----------+----------------------+---------------------------+------------+
| count(*) | googleResponseStored | parseGoogleResponseStatus | numResults |
+----------+----------------------+---------------------------+------------+
|   860738 |                    0 | NULL                      |       NULL |
|      350 |                    1 | NULL                      |       NULL |
+----------+----------------------+---------------------------+------------+
2 rows in set (3.66 sec)
```

This tells us that we have the google response for 350 queries.
Now we can re-execute the parser again for those 350 addresses (without querying google).

```
$ sbt "runMain BatchParserCmd --op=parseOnly --maxEntries=350 --dbUrl="$dbUrl" --tableName=addresses"

mysql> select count(*), googleResponse is not null as googleResponseStored, parseGoogleResponseStatus, numResults from addresses group by googleResponseStored, parseGoogleResponseStatus, numResults;
+----------+----------------------+-----------------------------------+------------+
| count(*) | googleResponseStored | parseGoogleResponseStatus         | numResults |
+----------+----------------------+-----------------------------------+------------+
|   860738 |                    0 | NULL                              |       NULL |
|       68 |                    1 | OK                                |          0 |
|       19 |                    1 | OK                                |          2 |
|      263 |                    1 | OK                                |          1 |
+----------+----------------------+-----------------------------------+------------+
4 rows in set (3.61 sec)
```

In case we want to force querying google again for any reason, we do as follows:
```
mysql> update addresses set googleResponse = null, parseGoogleResponseStatus = null, numResults = null;
$ sbt "runMain BatchParserCmd --op=googleQueryAndParse --maxEntries=250 --googleApiKey="$googleApiKey" --dbUrl="$dbUrl" --tableName=addresses"
```


# Customizing the parser
In order to extract more data from the google json response, you need to 
- modify the case classes `Result` and `ParsedAddress` (on `AddressParser.scala`)
- modify the function `parseAddressFromJsonResponse(googleResponseString: String): ParsedAddress` (on `AddressParser.scala`) 
- modify the function `saveGoogleResponseToDatabase(unformattedAddress: String, googleResponse: String)` (on `BatchParserCmd.scala`)
