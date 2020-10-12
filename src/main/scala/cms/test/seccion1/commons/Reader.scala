package cms.test.seccion1.commons

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

/**
 *
 */
class Reader {
  val logger: Logger = LogManager
    .getLogger("ETL - Data Pipeline - Test")
  /**
   *
   * @param pathFile
   * @param spark
   * @return
   */
  def readCSVFile(pathFile: String)(implicit spark: SparkSession): DataFrame = {
    val dfEmpty=spark.emptyDataFrame
    try {
      spark.read
        .option("inferSchema", "true") // Make sure to use string version of true
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("sep", ",")
        .csv(pathFile)
    }
    catch {
      case ex: AnalysisException =>
        logger.error(s"Check path about $ex ")
        dfEmpty
    }
  }
}
