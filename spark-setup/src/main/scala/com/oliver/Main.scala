package com.oliver

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object Main {

  def main(args: Array[String]) = {

                val conf = new SparkConf().setAppName("ScalaApp1").setMaster("local[1]")

                val sc = new SparkContext(conf)

                val session = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

                val path = "C:\\Users\\Asanka\\IdeaProjects\\youtube-vid-code\\spark-setup\\src\\main\\resources\\test.csv"

                val name = "C:/Users/Asanka/IdeaProjects/ScalaApp1/src/recources/"

                val query = "(SELECT * FROM dialog.fake_news) as mt"

                println("---------------  job1 start")
                Job1.job1(session, path)
                println("---------------  job1 end")

                println("---------------  job2 start")
                val df = Job2.job2(session, query)
                df.show()
                println("---------------  job2 end")

                val df1 = df.toDF()


                println("---------------  job3 start")
                df1.show()
                Job3.job3(session, df1, name)
                println("---------------  job3 end")


                session.stop()


  }

}
