package day2

import day1.*
import java.util.concurrent.atomic.*

// TODO: Copy the code from `FAABasedQueueSimplified`
// TODO: and implement the infinite array on a linked list
// TODO: of fixed-size `Segment`s.
class FAABasedQueue<E> : Queue<E> {
    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    init {
        val dummy = Segment(0)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val globalIndex = enqIdx.getAndIncrement()
            val segmentId = globalIndex / SEGMENT_SIZE

            var currentTail = tail.get()
            while (currentTail.id < segmentId) {
               var next = currentTail.next.get()
                if (next == null) {
                    val newSegment = Segment(currentTail.id + 1)
                    next = if (currentTail.next.compareAndSet(null, newSegment)) {
                        newSegment
                    } else {
                        currentTail.next.get()
                    }
                }

                tail.compareAndSet(currentTail, next)
                currentTail = tail.get()
            }

            val index = globalIndex % SEGMENT_SIZE
            if (tail.get().compareAndSet(index.toInt(), null, element)) {
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

            val globalIndex = deqIdx.getAndIncrement()
            val segmentId = globalIndex / SEGMENT_SIZE

            var currentHead = head.get()
            while (currentHead.id < segmentId) {
                currentHead = currentHead.next.get() ?: return null
            }

            val index = globalIndex % SEGMENT_SIZE
            val element = currentHead.getAndSet(index.toInt(), POISONED)
            if (element != null && element != POISONED) {
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

// TODO: Use me to mark a cell poisoned.
private val POISONED = Any()

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
