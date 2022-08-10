package com.oliver

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object Job1 {

  def job1(session: SparkSession, path: String) = {


    val schema = new StructType()
      .add("Id", IntegerType, true)
      .add("Title", StringType, true)
      .add("Author", StringType, true)
      .add("Text", StringType, true)

    val df = session.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")

      .schema(schema)
      .load(path)

    df.show()

    val dburl = "jdbc:postgresql://localhost:5432/testdb"

    val connectionProperties = new Properties()
    connectionProperties.put("user", s"asanka")
    connectionProperties.put("password", "111111")
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")

    df.write.mode(SaveMode.Overwrite).jdbc(dburl, "dialog.fake_news", connectionProperties)

  }
}
