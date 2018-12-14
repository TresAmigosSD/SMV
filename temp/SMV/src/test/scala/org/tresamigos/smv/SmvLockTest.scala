package org.tresamigos.smv

import scala.concurrent.duration._
import java.util.concurrent.TimeoutException

import SmvLock.withLock

class SmvLockTest extends SmvUnitSpec {
  val path = s"""${TmpDir}/locktest.lock"""
  val target = SmvLock(path)

  override def beforeEach = target.unlock()

  class TestThread(name: String, code: => Unit) extends Thread {
    var execTime = 0L
    override def run() =
      execTime = try {
        val start = System.currentTimeMillis
        withLock(path)(code)
        System.currentTimeMillis - start
      } catch {
        case _: Exception => -1L
      }
  }

  "SmvLock.lock()" should "not be reentrant" in {
    target.lock()
    try {
      val ex: Exception = intercept[IllegalStateException] { target.lock() }
      ex.getMessage shouldBe SmvLock.ReentranceDetectionMessage
    }
    finally { target.unlock() }
  }

  it should "block until the lock becomes available" in {
    // the first thread will sleep so that the second thread will be blocked
    val t1 = new TestThread("First Thread", Thread.sleep(1.second.toMillis))

    val t2 = new TestThread("Second Thread", {})

    t1.start()
    Thread.sleep(10) // to ensure the first thread will obtain the lock first
    t2.start()

    println("mutex blocking test will take some time while the second thread waits for lock release")
    t1.join()
    t2.join()

    assert(t1.execTime > 1.second.toMillis, "Expected first thread to release the lock within the set time")
    assert(t2.execTime > t1.execTime,
      s"Expected second thread to block till after the first thread completes, but it terminated in ${t2.execTime} millis")
  }

  "SmvLock.withLock" should "time out if lock cannot be obtained within a specified time" in {
    // first thread will obtain the lock first and hold it till secodn thread times out
    val time1 = 20.seconds.toMillis
    val t1 = new TestThread("First Thread", Thread.sleep(time1))

    // second thread will time out while waiting for the first thread to release the lock
    var execTime2 = 0L
    var except2: TimeoutException = null
    val time2 = 2000
    val t2 = new Thread(new Runnable {
      override def run() = {
        val start = System.currentTimeMillis
        try { withLock(path, time2){} } catch { case x: TimeoutException => except2 = x }
        execTime2 = System.currentTimeMillis - start
      }
    }, "Second Thread")

    t1.start()
    Thread.sleep(10)
    t2.start()

    println("mutex timeout test will take some time while the second thread waits for lock release")
    t2.join()
    assert(execTime2 < math.max(SmvLock.DefaultPollInterval, time2) + 100, "Expected second thread to time out")
    assert(except2 != null, "Expected TimeoutException in the blocking thread")
  }
}
