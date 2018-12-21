import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,SQLContext, SparkSession,Row,Column}
import org.apache.spark.sql.types._


package fpgrown.utils {

  import scala.collection.immutable.HashMap

  object func{

    def makeDict(session:SparkSession, frame:DataFrame): DataFrame ={
      val predict = frame.select("StockCode","Description")
        .groupBy("StockCode","Description").count()

      predict.select("StockCode","Description")
    }

    def groupingInvoice(session:SparkSession, frame: DataFrame):RDD[Array[String]] ={

      val grouped_data = frame.select("InvoiceNo","StockCode")
        .rdd
        .map(r => (r(0),r(1).toString()))
        .groupByKey()

      grouped_data.map(x=>x._2.toArray.distinct)
    }

  }
}