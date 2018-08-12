package com.CaseStudyIV.HospData

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._


object SparkHospData {

  def main(args: Array[String]): Unit = {

    //spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL - IV")
      .getOrCreate()

    // Removing all INFO logs in consol printing only result sets
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    println("spark session object is created")

    val Manual_schema = new StructType(Array( new StructField("DRGDefinition",StringType,
      true),
      new StructField("ProviderId", LongType, false),
      new StructField("ProviderName", StringType, true),
      new StructField("ProviderStreetAddress", StringType, false),
      new StructField("ProviderCity", StringType, false),
      new StructField("ProviderState", StringType, false),
      new StructField("ProviderZipCode", LongType, false),
      new StructField("HospitalReferralRegionDescription", StringType,true),
      new StructField("TotalDischarges", LongType, false),
      new StructField("AverageCoveredCharges", DoubleType, false),
      new StructField("AverageTotalPayments", DoubleType,false),
      new StructField("AverageMedicarePayments", DoubleType,false)))

    println(" Hospital Schema is created ")

    val Hospital_data = spark.read.format("csv")
      .option("header", "true")
      .schema(Manual_schema)
      .load("C:\\Users\\Shruthi\\Downloads\\inpatientCharges.csv").toDF()
    println("Hospitale Data top 20 rows ")
      Hospital_data.show()

    //Created View out of Hospital data to run Spark SQL queries
    Hospital_data.createOrReplaceTempView("Hospital_view")

    // What is the average amount of AverageCoveredCharges per state
    spark.sql(
      """select ProviderState, round(avg(AverageCoveredCharges),2) as
        |Avg_coveragecharges_state from Hospital_view group by ProviderState""".stripMargin).show()

    //find out the AverageTotalPayments charges per state
    spark.sql("""select ProviderState, round(sum(cast(AverageTotalPayments as
                decimal)/cast(pow(10,2) as decimal)),2) as Avg_tot_payment_state from Hospital_view
                group by ProviderState""").show()

    //find out the AverageMedicarePayments charges per state.
    spark.sql("""select ProviderState, round(sum(cast(AverageMedicarePayments as
                decimal)/cast(pow(10,2) as decimal)),2) as Avg_Medi_payment_state from Hospital_view
                group by ProviderState""").show()

    // Find out the total number of Discharges per state and for each disease
    // Sort the output in descending order of totalDischarges
    spark.sql("""select DRGDefinition,ProviderState, sum(TotalDischarges) as
                Total_Discharges from Hospital_view group by DRGDefinition, ProviderState order by
                Total_Discharges desc """).show()
  }
}
