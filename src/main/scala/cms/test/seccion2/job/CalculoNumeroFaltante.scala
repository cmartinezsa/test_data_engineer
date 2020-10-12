package cms.test.seccion2.job

import cms.test.seccion2.commons.Extract

class CalculoNumeroFaltante {

  def start(): Unit = {
    val extract = new Extract()
    val numNaturales = List.range(1, 101)
    val numToExtract = extract.validarNumeroIntroducido()
    extract.extractNumber(numNaturales, numToExtract)
  }

}
