package _PROJ_CLASS_.stage1

import org.tresamigos.smv._

/** define all the raw inputs into the stage */
// TODO: add extend SmvInputSet once that is implemented
object ExampleApp {
  val caBar = new CsvAttributes(delimiter = '|', hasHeader = true)

  val employment = SmvCsvFile("input/employment/CB1200CZ11.csv", caBar)
}

