package cms.test.seccion2.joboperation

import cms.test.seccion2.job.CalculoNumeroFaltante

object CalcularNumeroFaltante {

  def main(args: Array[String]): Unit = {
    val CNF = new CalculoNumeroFaltante()
    println("Iniciando App ")
    CNF.start()
    println("Finaliza App. ")
  }
}
