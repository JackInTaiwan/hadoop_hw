import scala.io.Source
import org.apache.spark.graphx.Graph
import scala.util.hashing.MurmurHash3



object PageRank {
    def main() {  
    //   val conf = new SparkConf().setAppName("pageRank")
    //   val sc = new SparkContext(conf)

    val fp = "/media/jack/File/hadoopHw/hw8/jack/network.txt"

    var links = List[Tuple2[Long, Long]]()
    var mapping = sc.textFile(fp)
    .flatMap(line => line.split(" "))
    .distinct
    .map(node => (MurmurHash3.stringHash(node).toLong, node))

    for (line <- Source.fromFile(fp).getLines) {
        var s = line.split(" ")(0)
        line.split(" ").drop(1).map(node => links = (MurmurHash3.stringHash(s).toLong, MurmurHash3.stringHash(node).toLong)::links)
    }

    var linksRdd = sc.parallelize(links).persist()

    val graph = Graph.fromEdgeTuples( linksRdd, 1 )

    val ranks = 
        graph
        .pageRank( 0.01 )
        .vertices

    val fullMap = 
    ranks
    .join( mapping )
    .map( row => row._2)
    .sortBy(_._2)
    .collect
    .foreach(println)
    }
}


PageRank.main()