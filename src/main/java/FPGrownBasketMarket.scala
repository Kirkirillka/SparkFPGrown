import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,SQLContext, SparkSession}
import org.apache.spark.sql.types._

import org.apache.log4j.Logger
import org.apache.log4j.Level
import fpgrown.utils.func.{makeDict,groupingInvoice}
import schema.dataSetSchema

object FPGrownBasketMarket {
  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val csvPath = "data/Online_Retail.csv"

    var sparkSession = SparkSession
      .builder()
      .appName("FPGrown")
      .master("local[*]")
      .getOrCreate()

    var market_basket = sparkSession.read
      .option("header","true")
      .option("delimiter",",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "false")
      .schema(dataSetSchema)
      .csv(csvPath)


    market_basket.printSchema()
    market_basket.show(1)


    val dictionary = makeDict(sparkSession,market_basket)

    // Prepare data for FP-growth algorithm
    val transactions = groupingInvoice(sparkSession, market_basket)
    //#transactions.map(x=>x.map(f=> f.toString()).mkString("[", ",", "]")).foreach(println)


    val fpg = new FPGrowth()
      .setMinSupport(0.02)
    val model = fpg.run(transactions)


    // print frequency for each item pair
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    // get association rules
    val minConfidence = 0.02
    val rules = model.generateAssociationRules(minConfidence)
      .sortBy(r => r.confidence, ascending = false)


    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(s => dictionary.where(col("StockCode").like(s)).select("Description")
            .first()
              .getString(0)).toList.mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => dictionary.where(col("StockCode").like(s)).select("Description")
          .first()
          .getString(0)).toList.mkString("[", ",", "]")
         + ", " + rule.confidence)
    }

  }
}
