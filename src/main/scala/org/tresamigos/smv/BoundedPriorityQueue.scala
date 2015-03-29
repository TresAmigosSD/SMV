package org.tresamigos.smv

import scala.collection.mutable

/**
 * Bounded priority queue trait that is intended to be mixed into instances of
 * scala.collection.mutable.PriorityQueue. By default PriorityQueue instances in
 * Scala are unbounded. This trait modifies the original PriorityQueue's
 * enqueue methods such that we only retain the top K elements.
 * The top K elements are defined by an implicit Ordering[A].
 * @author Ryan LeCompte (lecompte@gmail.com)
 *
 * Original Source: https://gist.github.com/ryanlecompte/5746241
 */
trait BoundedPriorityQueue[A] extends mutable.PriorityQueue[A] {
  def maxSize: Int

  override def +=(a: A): this.type = {
    if (size < maxSize) super.+=(a)
    else maybeReplaceLowest(a)
    this
  }

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def enqueue(elems: A*) {
    this ++= elems
  }

  private def maybeReplaceLowest(a: A) {
    // note: we use lt instead of gt here because the original
    // ordering used by this trait is reversed
    if (ord.lt(a, head)) {
      dequeue()
      super.+=(a)
    }
  }
}

object BoundedPriorityQueue {
  /**
   * Creates a new BoundedPriorityQueue instance.
   * @param maxElems the max number of elements
   * @return a new bounded priority queue instance
   */
  def apply[A: Ordering](maxElems: Int): BoundedPriorityQueue[A] = {
    // note: we reverse the ordering here because the mutable.PriorityQueue
    // class uses the highest element for its head/dequeue operations.
    val ordering = implicitly[Ordering[A]].reverse
    new mutable.PriorityQueue[A]()(ordering) with BoundedPriorityQueue[A] {
      implicit override val ord = ordering
      override val maxSize = maxElems
    }
  }
}
