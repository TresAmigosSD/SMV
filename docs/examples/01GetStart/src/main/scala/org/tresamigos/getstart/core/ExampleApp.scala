package org.tresamigos.getstart.core

import org.tresamigos.smv._

/** define all the raw inputs into the App */
object ExampleApp {
  val caBar = new CsvAttributes(delimiter = '|', hasHeader = true)

  val employment = SmvCsvFile("input/employment/CB1200CZ11.csv", caBar)
}

