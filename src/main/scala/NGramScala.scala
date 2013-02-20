
// ./spark-shell -J-Xmx1024m -cp ../CS149/CS149-pa3/class_dir/

import spark.SparkContext
import SparkContext._
import org.apache.hadoop.io.Text
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.Queue
import java.io._


object NGramScala extends Serializable{
    case class Article(title: String, score: Int){}

    def articleOrdering = new math.Ordering[Article] {
        def compare(art1: Article, art2: Article): Int = {
            if (art1.score < art2.score)
                return -1
            if (art1.score > art2.score)
                return +1
            return art1.title.compareTo(art2.title)
        }
    }

    def aggregateIntoQueue(queue: Queue[Article], article: Article ) = {
        var retQueue: Queue[Article] = queue
        if (queue.size < 20) {
            retQueue.enqueue(article)
            retQueue = retQueue.sorted(articleOrdering).toQueue
        }
        else {
            val minArticle = retQueue.head
            if (articleOrdering.compare(article,minArticle) > 0){
                retQueue.enqueue(article)

                retQueue = retQueue.sorted(articleOrdering).toQueue

                retQueue.dequeue
            }
        }
        retQueue
    }

    def mergeQueues(queue1: Queue[Article], queue2: Queue[Article]) = {
        var retQueue: Queue[Article] = queue1
        queue2 foreach { article => 
            retQueue = aggregateIntoQueue(retQueue, article)
        }
        retQueue
    }

    def main(args: Array[String]) {
        val sc = new SparkContext("local", "Simple Job")
        val wiki = sc.hadoopFile[Text, Text, ArticleInputFormat]("/usr/class/cs149/wikipedia/1gb")

        val query1 = sc.textFile("hdfs://myth1:47412/user/jamesbw/query/query1.txt")
        val query1str = query1.reduce( _ + "\n" + _ )
        val queryBag = new NGramBag(query1str, 4)
        val res = wiki.map{ case (title, article) => Article(title.toString, new NGramBag(article.toString, 4).score(queryBag))}
                      .filter(  _.score > 0)
        
        var queue: Queue[Article] = res.aggregate[Queue[Article]](new Queue[Article]())(aggregateIntoQueue, mergeQueues)
        queue = queue.sorted(articleOrdering).toQueue

        val saveStr = queue.toList.reverse.map{ article =>
            "Title: %s ; Score: %d\n" format (article.title, article.score) 
        }.mkString

        println(queue)

        val pw = new PrintWriter(new File("./scala_output"))
        pw.write(saveStr)
        pw.close()
    }
}
