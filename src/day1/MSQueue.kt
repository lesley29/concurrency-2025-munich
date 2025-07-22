package day1

import java.util.concurrent.atomic.AtomicReference

class MSQueue<E> : Queue<E> {
    private val head: AtomicReference<Node<E>>
    private val tail: AtomicReference<Node<E>>

    init {
        val dummy = Node<E>(null)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
//        TODO("implement me")
        val newTail = Node(element)
        while (true) {
            val currentTail = tail.get()
            if (currentTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(currentTail, newTail)
                return
            } else {
                tail.compareAndSet(currentTail, currentTail.next.get())
            }
        }
    }

    override fun dequeue(): E? {
//        TODO("implement me")
        while (true) {
            val currentHead = head.get()
            val nextHead = currentHead.next.get() ?: return null
            if (head.compareAndSet(currentHead, nextHead)) {
                val element = nextHead.element
                nextHead.element = null
                return element
            }
        }
    }

    // FOR TEST PURPOSE, DO NOT CHANGE IT.
    override fun validate() {
        check(tail.get().next.get() == null) {
            "`tail.next` must be `null`"
        }
        check(head.get().element == null) {
            "`head.element` must be `null`"
        }
    }

    private class Node<E>(
        var element: E?
    ) {
        val next = AtomicReference<Node<E>?>(null)
    }
}
