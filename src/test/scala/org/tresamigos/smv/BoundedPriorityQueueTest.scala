package org.tresamigos.smv

class BoundedPriorityQueueTest extends SparkTestUtil {
  test("Test bounded queue order") {
    val bpq = BoundedPriorityQueue[Int](3)
    List(5,2,4,1,3).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(3,4,5))
  }

  test("Test small bounded queue") {
    // create queue bigger than available num items.
    val bpq = BoundedPriorityQueue[Int](10)
    List(2,5,3,1,4).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(1,2,3,4,5))
  }

  test("Test duplicate values bounded queue") {
    val bpq = BoundedPriorityQueue[Int](4)
    List(3,5,2,3,4,3,1,3).foreach(i => bpq += i)
    assert(bpq.toList.sorted === List(3,3,4,5))
  }
}
