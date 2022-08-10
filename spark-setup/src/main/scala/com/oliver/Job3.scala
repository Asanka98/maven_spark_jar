package com.oliver

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object Job3 {

  def job3(session: SparkSession, dataFrame: sql.DataFrame, name: String): Unit = {

    val spark_impl = session
    dataFrame.show()

    dataFrame
      .write
      .format("csv")
      .option("header", true)
      .option("delimiter", ";")
      .option("sep", ",")
      .mode(SaveMode.Overwrite)
      .save(name)


  }

}
