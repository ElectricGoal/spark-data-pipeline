import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Ingestion {
    def main(agrs: Array[String]): Unit = {
        if (agrs.length < 3){
            println("Error, need 3 agurments !")
            System.exit(1)
        }

        var sourceFd = ""
        var saveFd = ""
        var executionDate = ""

        agrs.sliding(2, 2).toList.collect {
            case Array("--sourceFd", agrSourceFd: String) => sourceFd = agrSourceFd
            case Array("--saveFd", agrSaveFd: String) => saveFd = agrSaveFd
            case Array("--executionDate", agrExecutionDate: String) => executionDate = agrExecutionDate
        }

        println(sourceFd)
        println(saveFd)
        println(executionDate)

        val runtime = executionDate.split("-")
        val year = runtime(0)
        val month = runtime(1)
        val day = runtime(2)

        val spark = SparkSession
            .builder
            .appName("Ingesttion")
            .getOrCreate()

        val df = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(sourceFd)

        val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val saveLocation = s"/datalake/playground_datastore/$saveFd"
        val exists = fs.exists(new org.apache.hadoop.fs.Path(saveLocation))

        val outputDF = df
                .withColumn("year", lit(year))
                .withColumn("month", lit(month))
                .withColumn("day", lit(day))
            
        if (exists){
            println("File already exists, overwrite it !")
            outputDF
                .write
                .partitionBy("year", "month", "day")
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:9000" + saveLocation)
        }else{
            println("File not exists, create it !")
            outputDF
                .write
                .partitionBy("year", "month", "day")
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:9000" + saveLocation)
        }

    }
}