import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 *The ReducerBin class will perform specific operations on the values based on the key.
 * 1. If the key contains the string "All_venues"; then the respective venues are added along with all venues count from the values
 * 2. If the key contains the string "MMA"; then the max, the average and the median of the values are calculated
 * 3. If the key contains the string "Journal_Inproceedings_Year"; then the respective counts are added from the values
 * 4. If the key contains the string "Co-AuthorCount" or "AuthorScore"; then the values are added
*/
class ReducerBin extends Reducer[Text, Text, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    val Key = key.toString()
    val token = Key.split(",").toList(0)

    // 1.First Reduce operation
    if(token == "All_venues"){
      /**
     Each value in the list will hold the count for articles, inproceddings, proceedings, books, incollections, phdthesis, masterthesis and the total across all venues respectively
     */
      var count = List.fill(8)(0.toInt)

      for (value <- values.asScala){
        val temp = value.toString.split(",").toList
        count = (count, temp).zipped.map(_+_.toInt)
      }
      val reducer_value = count(0) + "," + count(1) +"," +count(2) + "," + count(3) + "," + count(4) + "," + count(5) +","+count(6)+","+count(7)

      logger.info("All_venues Key: ", key)
      context.write(key, new Text(reducer_value))
    }
      // 2.Second Reduce operation
      else if(token == "MMA"){
      val newval = values.asScala.map(_.toString.toInt)
      val sum = newval.sum
      val sorted_count = newval.toList.sorted
      val length = sorted_count.length
      val max = sorted_count.max  // Find maximum
      val average = (sum/length).toFloat   // Find average
      var median = 0.toFloat
      // Find median
      if(length%2 == 1){
        val index = (length+1)/2
        median = sorted_count(index-1).toFloat
      }
      else{
        val index = length/2
        median = ((sorted_count(index-1) + sorted_count(index))/2).toFloat
      }
      val reducer_value = max+"," + median + "," + average //+ " " + sorted_count

      logger.info("MMA Key: ", key)
      context.write(key, new Text(reducer_value))
    }
      // 3. Third reduce operation
      else if(token == "Journal_Inproceedings_Year"){
      /**
      Each value in the list will hold the count for journals, inproceddings, year(<90), year(91-00), year(01-19) respectively
       */
      var count = List.fill(5)(0.toInt)

      for (value <- values.asScala){
        val temp = value.toString.split(",").toList
        count = (count, temp).zipped.map(_+_.toInt)
      }
      val reducer_value = count(0) + "," + count(1) +"," +count(2) + "," + count(3) + "," + count(4)

      logger.info("Journal_Inproceedings_Year Key: ", key)
      context.write(key, new Text(reducer_value))
    }
    // 4. Fourth Reduce operation
    else if(token == "Co-AuthorCount" || token == "AuthorScore"){

      val newval = values.asScala.map(_.toString.toFloat).sum

      logger.info("Co-AuthorCount || AuthorScore Key: ", key)
      context.write(key, new Text(newval.toString))

    }
  }

}






