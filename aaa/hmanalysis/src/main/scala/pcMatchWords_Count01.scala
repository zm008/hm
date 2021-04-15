import java.util


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object pcMatchWords_Count01 {


  def main(args: Array[String]): Unit = {

    val con = new SparkConf().setAppName("pcMatchWords").setMaster("local")
    val sc = new SparkContext(con)

    //关键词
    val listRdd = sc.textFile("hdfs://hmcs030:9000/test/data/sensitive-word.txt")


    //PC数据
    val tablename = "hmcs_scan_webpage"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "hmcs030,hmcs031,hmcs032")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    //PC数据过滤
    val text: RDD[(String, String)] = hBaseRDD.map {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("p".getBytes, "c".getBytes))
        (key, name)
      }
    }.filter(f => {
      if (f._2 == null || f._2 == "") {
        false
      } else {
        true
      }
    })

    val flatmapRdd: RDD[(String, String)] = text.flatMapValues(
      t => {
        val strings: util.List[String] = JieBaFenCi.getJieBaFenci(t)
        strings.asScala
      }
    )
    val reduceRdd: RDD[((String, String), Int)] = flatmapRdd
      .map(t => {
        ((t._1, t._2), 1)
      }
      )
      .reduceByKey(_ + _)
    val mapRdd: RDD[(String, String)] = reduceRdd.map {
      case ((rowkey, word), count) => {
        (word, (rowkey + "_" + count))
      }
    }
    val hdfsRdd: RDD[(String, Int)] = listRdd.map((_,1))
    val joinRdd: RDD[(String, (String, Int))] = mapRdd.join(hdfsRdd)
    val resultRdd: RDD[(String, (String, Int))] = joinRdd.map {
      case (word, (rowkeycount, count)) => {
        val rowkeycnt: Array[String] = rowkeycount.split("_")
        (rowkeycnt(0), (word, count))
      }
    }


  }


}