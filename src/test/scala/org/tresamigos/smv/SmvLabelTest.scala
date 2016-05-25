package org.tresamigos.smv

class SmvLabelTest extends SmvTestUtil {
  def fixture = createSchemaRdd("id:Integer;name:String;sex:String", "1,Adam,male;2,Eve,female")

  test("labeling a column should not affect other columns") {
    val labeled = fixture.smvLabel("name")("white")
    labeled.smvGetLabels("name") shouldBe Seq("white")
    labeled.smvGetLabels("sex") shouldBe Seq.empty

    intercept[IllegalArgumentException] {
      labeled.smvGetLabels("does not exist")
    }
  }

  test("labeling columns should not alter the data in the data frame") {
    val df = fixture
    val labeled = df.smvLabel("name")("white")
    assertDataFramesEqual(df, labeled)
  }

  test("labeling a column should preserve previous meta data on the column") {
    val df1 = fixture
    val df2 = df1.selectPlus(df1("id") + 1 as "id1").smvDesc("id1" -> "id plus 1")
    val labeled = df2.smvLabel("id1")("purple")
    labeled.smvGetDesc() shouldBe Seq("id" -> "", "name" -> "", "sex" -> "", "id1" -> "id plus 1")
  }

  test("labeling a column twice should have the same effect as labeling once") {
    val label1 = fixture.smvLabel("name")("white")
    val label2 = fixture.smvLabel("name", "sex")("white")
    label2.smvGetLabels("name") shouldBe Seq("white")
    label2.smvGetLabels("sex") shouldBe Seq("white")
  }

  test("columns can have multiple labels") {
    val label = fixture.smvLabel("name")("white", "blue")
    label.smvGetLabels("name") shouldBe Seq("white", "blue")
  }

  test("removing labels should preserve other meta data") {
    val df1 = fixture
    val df2 = df1.selectPlus(df1("id") + 1 as "id1").smvDesc("id1" -> "id plus 1")
    val label1 = df2.smvLabel("id1")("white", "blue")
    val label2 = label1.smvRemoveLabel("id1")("white", "blue")
    label2.smvGetDesc() shouldBe Seq("id" -> "", "name" -> "", "sex" -> "", "id1" -> "id plus 1")
  }

  test("removing one label should preserve other labels") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val res = label2.smvRemoveLabel("name", "sex")("blue", "red")
    res.smvGetLabels("id") shouldBe 'Empty
    res.smvGetLabels("sex") shouldBe Seq("white")
    res.smvGetLabels("name") shouldBe Seq("white")
  }

  test("calling smvRemoveLabel with zero column arguments would remove the specified label from all columns") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res = label3.smvRemoveLabel()("white")
    res.smvGetLabels("id") shouldBe 'Empty
    res.smvGetLabels("name") shouldBe Seq("blue", "red")
    res.smvGetLabels("sex") shouldBe Seq("blue")
  }

  test("calling smvRemoveLabel with zero label argument would remove all labels from the specified columns") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res = label3.smvRemoveLabel("name", "sex")()
    res.smvGetLabels("id") shouldBe Seq("white")
    res.smvGetLabels("name") shouldBe 'Empty
    res.smvGetLabels("sex") shouldBe 'Empty
  }

  test("calling smvRemoveLabel with two empty parameter lists should clear all label meta data") {
    val label1 = fixture.smvLabel("name")("white", "blue", "red")
    val label2 = label1.smvLabel("sex")("white", "blue")
    val label3 = label2.smvLabel("id")("white")
    val res = label3.smvRemoveLabel()()
    res.smvGetLabels("id") shouldBe 'Empty
    res.smvGetLabels("name") shouldBe 'Empty
    res.smvGetLabels("sex") shouldBe 'Empty
  }

  test("selecting labeled columns should return only columns that have all the specified labels") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    val label2 = label1.smvLabel("name")("red")
    val res = label2.selectByLabel("white", "red")
    res.columns shouldBe Seq("name")
  }

  test("calling smvWithLabel with an empty argument list should return unlabeled columns") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    val res = label1.selectByLabel()
    res.columns shouldBe Seq("id")
  }

  test("smvWithLabel should throw instead of returning an empty list of columns") {
    val label1 = fixture.smvLabel("name", "sex")("white")
    intercept[IllegalArgumentException] {
      label1.smvWithLabel("nothing is labeled with this".split(" "):_*)
    }
  }

  test("adding description should be able to read out the same") {
    val df = fixture
    val res = df.smvDesc("id" -> "This is an ID field")
    res.smvGetDesc("id") shouldBe "This is an ID field"
    res.smvGetDesc() shouldBe Seq(("id", "This is an ID field"), ("name",""), ("sex", ""))
  }

  test("adding description to a column should preserve previous meta data on the column") {
    val df1 = fixture
    val labeled = df1.smvLabel("id")("purple")
    val res = labeled.smvDesc("id" -> "This is an ID field")
    res.smvGetLabels("id") shouldBe Seq("purple")
  }

  test("removeDesc should remove all desc with empty parameter") {
    val df1 = fixture
    val df2 = df1.smvDesc(
      "name" -> "a name",
      "id" -> "The ID"
    )
    val res = df2.smvRemoveDesc()
    res.smvGetDesc() shouldBe Seq(("id", ""), ("name",""), ("sex", ""))
  }
}
