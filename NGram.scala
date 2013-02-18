
// ./spark-shell -J-Xmx1024m -cp ../CS149/CS149-pa3/class_dir/

import spark.SparkContext
import SparkContext._
import org.apache.hadoop.io.Text
import scala.collection.mutable.PriorityQueue

object NGram extends Application {
	case class Article(title: String, score: Int) {}

	implicit def articleOrdering: Ordering[Article] = new Ordering[Article] {
		def compare(art1: Article, art2: Article): Int = {
			if (art1.score < art2.score)
				return 1
			if (art1.score > art2.score)
				return -1
			return - art1.title.compareTo(art2.title)
		}
	}

	def aggregateIntoQueue(queue: PriorityQueue[Article], article: Article ) {
		if (queue.size < 20) {
			queue.enqueue(article)
		}
		else {
			val minArticle = queue.head
			if (articleOrdering.compare(article,minArticle) > 0){
				queue.enqueue(article)
				queue.dequeue
			}
		}
	}

	def mergeQueues(queue1: PriorityQueue[Article], queue2: PriorityQueue[Article]) {
		queue2 foreach { article => 
			aggregateIntoQueue(queue1, article)
		}
	}

	val logFile = "./log" // Should be some file on your system
	val sc = new SparkContext("local", "Simple Job")
	val wiki = sc.hadoopFile[Text, Text, ArticleInputFormat]("/usr/class/cs149/wikipedia/1gb")
	val query1 = sc.textFile("hdfs://myth1:47412/user/jamesbw/query/query1.txt")
	val query1str = query1.reduce( _ + _ )
	val queryBag = new NGramBag(query1str, 4)
	val res = wiki.map{ case (title, article) => (title.toString, new NGramBag(article.toString, 4).score(queryBag))}.filter(  _._2 > 0)
	val queue = new PriorityQueue[Article]()(articleOrdering)

	res.aggregate[PriorityQueue[Article]](new PriorityQueue[Article]())(aggregateIntoQueue, mergeQueues)

	queue.toList.reverse.foreach(println)
		
	
}

//val wiki = sc.hadoopFile[Text, Text, ArticleInputFormat]("hdfs://myth1:47412/user/jamesbw/input/chunk_aa")




//val max = res.map(_._2).reduce(Math.max)

//val topMatch = res.filter(_._2 == max).first()