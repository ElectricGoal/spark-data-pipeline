import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import org.apache.spark.sql.hive.HiveContext


object Transformation{
    def main(agrs: Array[String]): Unit = {
        if (agrs.length < 2){
            println("Error, need 2 agurments !")
            System.exit(1)
        }

        var sourceFd = ""
        var tableName = ""
        var executionDate = ""
        agrs.sliding(2, 2).toList.collect {
            case Array("--sourceFd", agrSourceFd: String) => sourceFd = agrSourceFd
            case Array("--tableName", agrTableName: String) => tableName = agrTableName
            case Array("--executionDate", agrExecutionDate: String) => executionDate = agrExecutionDate
        }

        val runtime = executionDate.split("-")
        val year = runtime(0)
        val month = runtime(1)
        val day = runtime(2)

        // create spark session for hive
        val spark = SparkSession
            .builder
            .appName("Transformation")
            .config("hive.metastore.uris", "thrift://localhost:9083")
            .config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            // .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate()

        // show hive databases
        spark.sql("use test").show()
        spark.sql("show tables").show()

        // read data from hdfs
        val df = spark
                    .read.parquet("hdfs://localhost:9000/datalake/playground_datastore/" + sourceFd)
                    // .drop("year", "month", "day")

        df.schema.printTreeString()

        // transform data
        // val df2 = df
        //     .withColumn("movieId", lit("movieId"))
        //     .withColumn("title", lit("title"))
        //     .withColumn("genres", lit("genres"))

        // write data to hive
        df.write
            .format("hive")
            .partitionBy("year", "month", "day")
            .mode(SaveMode.Append)
            .saveAsTable(tableName)
    }
}