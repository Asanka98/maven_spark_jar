package com.oliver

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Job2 {

  def job2(session: SparkSession, query: String): sql.DataFrame = {


    val dburl = "jdbc:postgresql://localhost:5432/testdb"

    val connectionProperties = new Properties()
    connectionProperties.put("user", s"asanka")
    connectionProperties.put("password", "111111")
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")


    val df_from_postgres = session.read.jdbc(dburl, query, connectionProperties)

    //    df_from_postgres.show()

    df_from_postgres
  }

}
