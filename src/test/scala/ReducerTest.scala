import org.apache.hadoop.io.{Text}
import org.mockito.Mockito.verify
import org.scalatest.FlatSpec
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ReducerTest extends FlatSpec with MockitoSugar{

  val reducer = new ReducerBin

  "reduce" should "output co-author count to respective bins" in {
    val context = mock[reducer.Context]
     reducer.reduce(
      key = new Text("Co-AuthorCount,"+"BIN_2"),
      values = Seq(new Text(2.toString), new Text(3.toString)).asJava,
      context
    )
    verify(context).write(new Text("Co-AuthorCount,"+"BIN_2"), new Text(5.0.toString))
  }

}
