package day2

import day1.*
import java.util.concurrent.atomic.*

// TODO: Copy the code from `FAABasedQueueSimplified`
// TODO: and implement the infinite array on a linked list
// TODO: of fixed-size `Segment`s.
class FAABasedQueue<E> : Queue<E> {
    private val infiniteArray = InfiniteArray<Any?>()
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    override fun enqueue(element: E) {
        while (true) {
            val index = enqIdx.getAndIncrement()
            if (infiniteArray.compareAndSet(index.toInt(), null, element)) {
                break
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (isEmpty()) {
                return null
            }
            val index = deqIdx.getAndIncrement().toInt()
            val element = infiniteArray.getAndSet(index, POISONED)
            if (element != null && element != POISONED) {
//                infiniteArray.shrinkTo(index)
                return element as? E?
            }
        }
    }

    private fun isEmpty() = deqIdx.get() >= enqIdx.get()
}

// TODO: Use me to construct a linked list of segments.
private class Segment(val id: Long) : AtomicReferenceArray<Any?>(SEGMENT_SIZE) {
    val next = AtomicReference<Segment?>(null)
}

private class InfiniteArray<E> {
    private val head: AtomicReference<Segment>

    init {
        val dummy = Segment(0)
        head = AtomicReference(dummy)
    }

    fun compareAndSet(index: Int, expected: E, value: E): Boolean {
        val segmentId = getSegmentId(index)
        val segment = findSegment(head.get(), segmentId)
        val localIndex = getLocalIndex(index)
        return segment.compareAndSet(localIndex, expected, value)
    }

    fun getAndSet(index: Int, value: E): Any? {
        val segmentId = getSegmentId(index)
        val segment = findSegment(head.get(), segmentId)
        val localIndex = getLocalIndex(index)
        return segment.getAndSet(localIndex, value)
    }

    fun shrinkTo(globalIndex: Int) {
        val stopAt = getSegmentId(globalIndex)
        while (true) {
            val current = this.head.get()
            if (current.id >= stopAt && current.next.get() == null) {
                break
            }
            head.compareAndSet(current, current.next.get())
        }
    }

    private fun getSegmentId(globalIndex: Int) = globalIndex / SEGMENT_SIZE.toLong()

    private fun getLocalIndex(globalIndex: Int) = globalIndex % SEGMENT_SIZE

    private fun findSegment(start: Segment, id: Long): Segment {
        var current: Segment = start
        while (true) {
            if (current.id == id) {
                return current
            }

            val next = current.next.get()
            if (next != null) {
                current = next
                continue
            }

            val new = Segment(current.id + 1)
            if (current.next.compareAndSet(null, new)) {
                return new
            }
        }
    }
}

// TODO: Use me to mark a cell poisoned.
private val POISONED = Any()

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
