
import org.apache.hadoop.io.{LongWritable, Text}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatestplus.mockito._

class MapperTest extends FlatSpec with MockitoSugar  {

  val mapper = new MapperBin

  "map" should "output author to respective bins" in {
    val context = mock[mapper.Context]
    val content = "<inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Ugo Buy</author>\n<author>B. M. Mainul Hossain</author>\n<author>Mark Grechanik</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    mapper.map(
      key = new LongWritable(),
      value = new Text(content),
      context
    )
    verify(context).write(new Text("Co-AuthorCount,"+"BIN_2"), new Text(1.toString))
  }

  "map" should "output author score" in {
    val context = mock[mapper.Context]
    val content = "<inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Ugo Buy</author>\n<author>B. M. Mainul Hossain</author>\n<author>Mark Grechanik</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    mapper.map(
      key = new LongWritable(),
      value = new Text(content),
      context
    )

    verify(context).write(new Text("AuthorScore,"+"Ugo Buy"), new Text(0.41667.toString))
  }

  "map" should "output venue count" in {
    val context = mock[mapper.Context]
    val content = "<article mdate=\"2018-06-28\" key=\"tr/sql/X3H2-90-412\" publtype=\"informal\">\n<author>David Beech</author>\n<author>Cetin Ozbutun</author>\n<title>Object Oriented DBMS as a Generalization of Relational DBMS</title>\n<journal>ANSI X3H2</journal>\n<volume>X3H2-90-412</volume>\n<year>1990</year>\n<url>db/ansi/x3h2.html#X3H2-90-412</url>\n<cdrom>SQL/X3H2-90-412.pdf</cdrom>\n</article>"
    mapper.map(
      key = new LongWritable(),
      value = new Text(content),
      context
    )
    verify(context).write(new Text("All_venues,"+"BIN_2"), new Text("1,0,0,0,0,0,0,1"))
  }

  "map" should "output number of co-authors" in {
    val context = mock[mapper.Context]
    val content = "<article mdate=\"2018-06-28\" key=\"tr/sql/X3H2-90-412\" publtype=\"informal\">\n<author>David Beech</author>\n<author>Cetin Ozbutun</author>\n<title>Object Oriented DBMS as a Generalization of Relational DBMS</title>\n<journal>ANSI X3H2</journal>\n<volume>X3H2-90-412</volume>\n<year>1990</year>\n<url>db/ansi/x3h2.html#X3H2-90-412</url>\n<cdrom>SQL/X3H2-90-412.pdf</cdrom>\n</article>"
    mapper.map(
      key = new LongWritable(),
      value = new Text(content),
      context
    )
    verify(context).write(new Text("MMA,"+"David Beech"), new Text(2.toString))
  }

  "map" should "output journal, inproceedings and year range count" in {
    val context = mock[mapper.Context]
    val content = "<article mdate=\"2017-05-27\" key=\"journals/fmsd/SloanB97\">\n<author>Ugo A. Buy</author>\n<author>Robert H. Sloan</author>\n<title>Stubborn Sets for Real-Time Petri Nets.</title>\n<pages>23-40</pages>\n<year>1997</year>\n<volume>11</volume>\n<journal>Formal Methods in System Design</journal>\n<number>1</number>\n<url>db/journals/fmsd/fmsd11.html#SloanB97</url>\n<ee>https://doi.org/10.1023/A:1008629725384</ee>\n</article>"
    mapper.map(
      key = new LongWritable(),
      value = new Text(content),
      context
    )
    verify(context).write(new Text("Journal_Inproceedings_Year,"+"BIN_2"), new Text("1,0,0,1,0"))
  }






}
