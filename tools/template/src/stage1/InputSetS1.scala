package _PROJ_CLASS_.stage1

import org.tresamigos.smv._

/** define all the raw inputs into the stage */

private object CA {
  val caBar = new CsvAttributes(delimiter = '|', hasHeader = true)

}

object employment extends SmvCsvFile("input/employment/CB1200CZ11.csv", CA.caBar)

