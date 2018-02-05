package org.tresamigos.smv

import scala.concurrent.duration._
import java.util.concurrent.TimeoutException

/**
 * Provides a file-based mutex control, or non-reentrant lock.
 *
 * A typical use should use the `SmvLock.withLock` method in the companion
 * object, or follow the idiom below:
 *
 * <code>
 *   val sl = SmvLock("/path/to/my/file.lock")
 *   sl.lock()
 *   try { // access lock-protected resource } finally { sl.unlock() }
 * </code>
 *
 * The parenthese `()` is recommended to indicate use of side effect.
 */
case class SmvLock(path: String, timeout: Long = Long.MaxValue) {
  private var obtained = false
  private var attempts = 0;

  /** Tries to acquire the lock if it is available within the given timeout, block untill successful */
  def lock(): Unit = {
    if (obtained) throw new IllegalStateException(SmvLock.ReentranceDetectionMessage)

    val start = System.currentTimeMillis
    while (! obtained) {
      attempts += 1
      try {
        SmvHDFS.createFileAtomic(path)
        obtained = true
      }
      catch {
        case _: Exception =>
          if (System.currentTimeMillis - start > timeout)
            throw new TimeoutException(s"Cannot obtain lock [${path}] within ${timeout/1000} seconds")

          if (1 == attempts)
            System.out.println(f"Found lock file [${path}] created on ${new java.util.Date()}")

          try {
            // TODO: implement truncated exponential backoff in sleep time
            Thread.sleep(SmvLock.DefaultPollInterval)
          }
          catch {
            case _: InterruptedException => // getting waken up is okay, check if lock is available again
          }
      }
    }
  }

  /** Releases the lock */
  def unlock(): Unit = {
    SmvHDFS.deleteFile(path)
  }
}

object SmvLock {
  val ReentranceDetectionMessage = "Non-reentrant lock already obtained"

  // TODO: make sleep interval configurable
  val DefaultPollInterval = 10.seconds.toMillis

  /** Convenience method for using the lock */
  def withLock[T](path: String, timeout: Long = Long.MaxValue)(code: => T) = {
    val sl = SmvLock(path, timeout)
    sl.lock()
    try { code } finally { sl.unlock() }
  }
}
