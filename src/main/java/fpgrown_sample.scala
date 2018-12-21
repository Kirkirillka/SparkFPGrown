import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


import org.apache.log4j.Logger
import org.apache.log4j.Level


class DataSetSQL(sparkSession: SparkSession, input: String) {
  var dataSetSchema = StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", StringType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", IntegerType, true),
    StructField("Country", StringType, true)))

  //Read CSV file to DF and define scheme on the fly
  private val marketBasket = sparkSession.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    //.schema(dataSetSchema)
    .csv(input)

  marketBasket.createOrReplaceTempView("dataSetTable")

  marketBasket.printSchema()
  marketBasket.show()
  //Find the most mentioned actors (persons)
  def getInvoiceNoAndStockCode(): DataFrame = {
    sparkSession.sql(
      " SELECT InvoiceNo, StockCode" +
        " FROM dataSetTable" +
        " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
  }

  def getStockCodeAndDescription(): DataFrame = {
    sparkSession.sql(
      "SELECT DISTINCT StockCode, Description" +
        " FROM dataSetTable" +
        " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
  }
}


object FpGrowthRetail {

  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFile = "data/retail2.csv"

    val session = SparkSession
      .builder()
      .appName("spark-fpgrowth")
      .master("local[*]")
      .getOrCreate()

    //read file with purchases
    //val fileRDD: RDD[String] = sc.textFile(inputFile)

    //get transactions
    //val products: RDD[Array[String]] = fileRDD.map(s => s.split(";"))

    val dataSetSQL = new DataSetSQL(session, inputFile)
    val dataFrameOfInvoicesAndStockCodes = dataSetSQL.getInvoiceNoAndStockCode()

    val keyValue = dataFrameOfInvoicesAndStockCodes.rdd.map(row => (row(0), row(1).toString))
    val groupedKeyValue = keyValue.groupByKey()
    val transactions = groupedKeyValue.map(row => row._2.toArray.distinct)

    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth()
      .setMinSupport(0.01)

    val model = fpg.run(transactions)

    model.freqItemsets.collect.foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.09
    val rules2 = model.generateAssociationRules(minConfidence)
    val rules = rules2.sortBy(r => r.confidence, ascending = false)

    val dataFrameOfStockCodeAndDescription = dataSetSQL.getStockCodeAndDescription()
    val dictionary = dataFrameOfStockCodeAndDescription.rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(s => dictionary(s)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => dictionary(s)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}