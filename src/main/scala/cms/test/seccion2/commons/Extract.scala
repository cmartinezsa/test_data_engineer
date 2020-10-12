package cms.test.seccion2.commons

class Extract {
  def extractNumber(listNums: List[Int], numToExtract: Int): Unit = {
    println("Lista de Numeros Naturales : ".concat(listNums.toString()))
    //Filtrar el elemento a extraer
    val newListNums = listNums.filter(_ != numToExtract)
    println("Nueva Lista de numeros naturales; sin el numero [" + numToExtract + "] :" + newListNums.toString())
    //Comparar las dos listas y obtener los elementos que no esta presente en la lista anterior.
    val listaFinal = listNums.filter(!newListNums.contains(_))

    //Validar el numero extraido vs el solicitado por consola
    if ((numToExtract.toString.equals(listaFinal.mkString))) {
      println("Numero a  extraer: ".concat(numToExtract.toString)
        .concat(" Numero Extraido : ") + listaFinal.mkString)
      println("El numero extraido, corresponde al numero solicitado.")
    }
    else {
      println("El numero extraido, no corresponde al numero solicitado.")
    }
  }

  def validarNumeroIntroducido(): Int = {
    val scanner = new java.util.Scanner(System.in)
    val zero = 0
    var num = ""
    print("Introducir numero natural entre 1 - 100: ")
    try {
      num = scanner.nextLine()
      while (isNumericInt(num) == 0 || num.toInt < 1 || num.toInt > 100) {
        print("Numero invalido [ ".concat(num.toString)
          .concat(" ]. ¡Favor de introducir un numero entre 1 - 100! :"))
        num = scanner.nextLine()
      }
      println("Numero valido: ".concat(num.toString))
      num.toInt
    }
    catch {
      case e: NumberFormatException => {
        println("El caracter introducido, no corresponde a un valor numerico.")
        num = scanner.nextLine()
        zero
      }
    }
  }

  def isNumericInt(str: String): Int = {
    val zero = 0
    try {
      str.toInt
    }
    catch {
      case e: NumberFormatException => {
        println("El caracter introducido no corresponde a un valor entero.")
        zero
      }
    }
  }
}

