package day1

import java.util.concurrent.atomic.AtomicReference

class TreiberStack<E> : Stack<E> {
    // Initially, the stack is empty.
    private val top = AtomicReference<Node<E>?>(null)

    override fun push(element: E) {
        // TODO: Make me linearizable!
        // TODO: Update `top` via Compare-and-Set,
        // TODO: restarting the operation on CAS failure.
        var curTop = top.get()
        var newTop = Node(element, curTop)
        while (!top.compareAndSet(curTop, newTop)) {
            curTop = top.get()
            newTop = Node(element, curTop)
        }
    }

    override fun pop(): E? {
        // TODO: Make me linearizable!
        // TODO: Update `top` via Compare-and-Set,
        // TODO: restarting the operation on CAS failure.
        var curTop = top.get() ?: return null
        while (!top.compareAndSet(curTop, curTop.next)) {
           curTop = top.get() ?: return null
        }
        return curTop.element
    }

    private class Node<E>(
        val element: E,
        val next: Node<E>?
    )
}