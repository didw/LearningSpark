import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
object SimpleGraphApp {
  def main(args: Array[String]){

    // Configure the program 
    val conf = new SparkConf()
          .setAppName("Tiny Social")
          .setMaster("local")
          .set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)

    // Load some data into RDDs
    val people = sc.textFile("./data/people.csv")
    val links = sc.textFile("./data/links.csv")
 
    // After that, we use the Spark API as in the shell
    // ...
      case class Person(name: String, age: Int)
      val peopleRDD: RDD[(VertexId, Person)] = people map { line => 
          val row = line split ','
          (row(0).toInt, Person(row(1), row(2).toInt))
        }
      type Connection = String
      val linksRDD: RDD[Edge[Connection]] = links map {line => 
          val row = line split ','
          Edge(row(0).toInt, row(1).toInt, row(2))
        }
      println("test")
      val tinySocial: Graph[Person, Connection] = Graph(peopleRDD, linksRDD)
      val profLinks: List[Connection] = List("coworker", "boss", "employee","client", "supplier")
      tinySocial.subgraph(profLinks contains _.attr).
     triplets.foreach(t => println(t.srcAttr.name + " is a " + t.attr + " of " + t.dstAttr.name))
  }
}
