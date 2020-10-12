package cms.test.seccion1.joboperation

import cms.test.seccion1.commons.Atributos
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import cms.test.seccion1.job.EtlTableCargo

object ETLJobOperator extends  Atributos{
  def main(args: Array[String]): Unit = {
    val PATH = "/home/my_user/IdeaProjects/TestSeccionDos/src/test/resources/data_prueba_tecnica.csv"
    val ETLCARGO = new EtlTableCargo()
    val LOGGER: Logger = LogManager
      .getLogger(msjLog)

    LOGGER.info("Iniciando Proceso")
    val SPARK = SparkSession
      .builder()
      .config("spark.service.user.postgresql.pass", "my_password")
      .config("spark.service.user.postgresql.user", "my_user_db")
      .config("spark.service.user.postgresql.database", "db_test_daen")
      .config("spark.service.user.postgresql.port", "5432")
      .config("spark.service.user.postgresql.host", "localhost")
      .master("local")
      .appName("ETL - Data Pipeline - Test")
      .getOrCreate()
    ETLCARGO.transformTableCargo(PATH)(SPARK)
  }
}
