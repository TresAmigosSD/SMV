package org.tresamigos.smv
package util

import StringConversion._

class StringConversionTest extends SmvUnitSpec {
  "StringConversion" should "be able to convert integers" in {
    canConvertToInt("a") shouldBe false
    canConvertToInt("1.0") shouldBe false
    canConvertToInt("123") shouldBe true
  }

  it should "use Long when the number approaches threshold for Int" in {
    val s = (IntThreshold + 1).toString
    canConvertToInt(s) shouldBe false
    canConvertToLong(s) shouldBe true
  }

  it should "be able to convert floats" in {
    canConvertToFloat("a") shouldBe false
    canConvertToFloat("2") shouldBe true
    canConvertToFloat("3.14") shouldBe true
  }

  it should "use doble when the number approaches threshold for Float" in {
    val s = (FloatThreshold + 0.1f).toString
    canConvertToFloat(s) shouldBe false
    canConvertToDouble(s) shouldBe true
  }
}
