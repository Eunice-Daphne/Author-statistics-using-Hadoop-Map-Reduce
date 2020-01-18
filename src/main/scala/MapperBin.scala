import com.typesafe.config.ConfigFactory
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
The MapperBin class will output the key-value pairs for all the required statistics through parallel processing. We attach a string uniquely representing each of the statistics with the key when writing to the mapper.
The following are the required statistics.
 1. Each bin shows the range of the numbers of co-authors (e.g., the first bin(BIN_1) is one, second bin(BIN_2) is 2-3, third(BIN_3) is 4-6, fourth(BIN_4) is 7-10, fifth(BIN_5) is 11-15 and the rest in sixth(BIN_6))
 2. Score for each author calculated based on the following
    "The score of the last co-author is credited 1/(4N) leaving it 3N/4 of the original score. The next co-author to the left is debited 1/(4N) and the process repeats until the first author is reached"
 3. A stratified breakdown of the statistics by publication venues in addition to the cumulative statistics across all venues. The statistics is based on the number of co-authors.
 4. A histogtram stratified by journals, inproceedings, and years of publications
*/
class MapperBin extends Mapper[LongWritable, Text, Text, Text] {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val dblpdtdPath = getClass.getClassLoader.getResource("dblp.dtd").toURI

  private val parserFactory = SAXParserFactory.newInstance
  private val parser = parserFactory.newSAXParser

  val conf = ConfigFactory.load("TagConf")
  val all_venues =  conf.getString("All_venues")


  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    var allVenuesCount : String = null
    var bin : String = null
    var journal_count : String = 0.toString
    var inproceedings_count : String = 0.toString
    var yearRangeLessThan90 : String = 0.toString
    var yearRangeBtw91To00 : String = 0.toString
    var yearRangeBtw01To19 : String = 0.toString

    // Parse the input XML using XMLInputForamt for multiple start and end tags
    val loadInputXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dblpdtdPath"><dblp>""" + value.toString + "</dblp>"
    val content = XML.withSAXParser(parser).loadString(loadInputXML)

    logger.info("The parsed XML input : " + content)

    val Authors = (content \\ "author").map(author => author.text.trim).toList
    val number_of_authors = Authors.length
    logger.info("Number of authors in the content" + number_of_authors)

    //1.First Statistics
    logger.debug("Statistics for First Histogram")
    val init_token = "Co-AuthorCount,"
    if (number_of_authors==1){
      context.write(new Text(init_token+"BIN_1"), new Text(1.toString))
    }
    else if (number_of_authors>1 && number_of_authors<=3){
      context.write(new Text(init_token+"BIN_2"), new Text(1.toString))
    }
    else if (number_of_authors>=4 && number_of_authors<=6){
      context.write(new Text(init_token+"BIN_3"), new Text(1.toString))
    }
    else if (number_of_authors>=7 && number_of_authors<=10){
      context.write(new Text(init_token+"BIN_4"), new Text(1.toString))
    }
    else if (number_of_authors>=11 && number_of_authors<=15){
      context.write(new Text(init_token+"BIN_5"), new Text(1.toString))
    }
    else {
      context.write(new Text(init_token+"BIN_6"), new Text(1.toString))
    }

    //2. Second Statistics
    logger.debug("Author Score for each author based on co-author count")
    val score = new Array[Float](Authors.length)
    for (i <- Authors.indices) {
      if (i == 0) {
        score(i) = (1.toFloat / number_of_authors) + (1.toFloat / (4.toFloat * number_of_authors))
      }
      else if (i == number_of_authors - 1) {
        score(i) = (1.toFloat / number_of_authors) - (1.toFloat / (4.toFloat * number_of_authors))
      }
      else {
        score(i) = 1.toFloat / number_of_authors
      }

      val score_f = score(i)
      val str = f"$score_f%.5f"

      context.write(new Text("AuthorScore," + Authors(i)), new Text(str))
    }
      //3. Third Statistics
      //val venues = List("article", "inproceedings", "proceedings", "book", "incollection", "phdthesis", "mastersthesis")
      logger.debug("Count of records in each venues")
      val venues = all_venues.split(",").toList
      val venue = content.child.head.label
      logger.info("Venue for this XML record content" + venue)

      if (venue == venues(0)) {allVenuesCount = "1,0,0,0,0,0,0,1"}
      else if(venue == venues(1)){allVenuesCount = "0,1,0,0,0,0,0,1"}
      else if(venue == venues(2)){allVenuesCount = "0,0,1,0,0,0,0,1"}
      else if(venue == venues(3)){allVenuesCount = "0,0,0,1,0,0,0,1"}
      else if(venue == venues(4)){allVenuesCount = "0,0,0,0,1,0,0,1"}
      else if(venue == venues(5)){allVenuesCount = "0,0,0,0,0,1,0,1"}
      else if(venue == venues(6)){allVenuesCount = "0,0,0,0,0,0,1,1"}

      if (number_of_authors==1){bin = "BIN_1"}
      else if (number_of_authors>1 && number_of_authors<=3){bin = "BIN_2"}
      else if (number_of_authors>=4 && number_of_authors<=6){bin = "BIN_3"}
      else if (number_of_authors>=7 && number_of_authors<=10){bin = "BIN_4"}
      else if (number_of_authors>=11 && number_of_authors<=15){bin = "BIN_5"}
      else {bin = "BIN_6"}

      context.write(new Text("All_venues,"+bin), new Text(allVenuesCount))

      for (author <- Authors){
        context.write(new Text("MMA,"+author), new Text(number_of_authors.toString))
    }
    //4. Fourth Statistics
    logger.debug("Count of journals, inproceedings and year ranges")
    val journal = (content \\ "journal").text.trim
    logger.info("Journal for this XML record content" + journal)

    if(journal != null && journal != "" && journal != " "){
      journal_count = 1.toString
    }
    if(content.child.head.label == "inproceedings"){
      inproceedings_count = 1.toString
    }

    val year = (content \\ "year").text
    logger.info("Year for this XML record content" + year)

    if(year.nonEmpty) {
      if (year.toInt <= 1990) {
        yearRangeLessThan90 = 1.toString
      }
      else if (year.toInt > 1990 && year.toInt <= 2000) {
        yearRangeBtw91To00 = 1.toString
      }
      else if (year.toInt > 2000 && year.toInt <= 2019) {
        yearRangeBtw01To19 = 1.toString
      }
    }
    context.write(new Text("Journal_Inproceedings_Year,"+bin), new Text(journal_count+","+inproceedings_count+","+yearRangeLessThan90+","+yearRangeBtw91To00+","+yearRangeBtw01To19))

    }




}
