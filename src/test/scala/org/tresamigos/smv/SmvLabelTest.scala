package org.tresamigos.smv

class SmvLabelTest extends SmvTestUtil {
  def fixture = dfFrom("id:Integer;name:String;sex:String", "1,Adam,male;2,Eve,female")
  def descriptionDF =
    dfFrom("variables:String;decriptions:String",
           "id,This is an ID field;name,This is a name field;sex,This is a sex filed")

  test(
    "calling smvRemoveLabel with zero column arguments would remove the specified label from all columns") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res    = label3.smvRemoveLabel()("white")
    res.smvGetLabels("id") shouldBe 'Empty
    res.smvGetLabels("name") shouldBe Seq("blue", "red")
    res.smvGetLabels("sex") shouldBe Seq("blue")
  }

  test(
    "calling smvRemoveLabel with zero label argument would remove all labels from the specified columns") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res    = label3.smvRemoveLabel("name", "sex")()
    res.smvGetLabels("id") shouldBe Seq("white")
    res.smvGetLabels("name") shouldBe 'Empty
    res.smvGetLabels("sex") shouldBe 'Empty
  }

  test("calling smvRemoveLabel with two empty parameter lists should clear all label meta data") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res    = label3.smvRemoveLabel()()
    res.smvGetLabels("id") shouldBe 'Empty
    res.smvGetLabels("name") shouldBe 'Empty
    res.smvGetLabels("sex") shouldBe 'Empty
  }

  test("selecting labeled columns should return only columns that have all the specified labels") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    val label2 = label1.smvLabel("name")("red")
    val res    = label2.selectByLabel("white", "red")
    res.columns shouldBe Seq("name")
  }

  test("calling smvWithLabel with an empty argument list should return unlabeled columns") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    val res    = label1.selectByLabel()
    res.columns shouldBe Seq("id")
  }

  test("smvWithLabel should throw instead of returning an empty list of columns") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    intercept[IllegalArgumentException] {
      label1.smvWithLabel("nothing is labeled with this".split(" "): _*)
    }
  }

  test("adding description to a column should preserve previous meta data on the column") {
    val df1     = fixture
    val labeled = df1.smvLabel("id")("purple")
    val res     = labeled.smvDesc("id" -> "This is an ID field")
    res.smvGetLabels("id") shouldBe Seq("purple")
  }

}
