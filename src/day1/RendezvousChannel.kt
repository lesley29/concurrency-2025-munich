package day1

import java.util.concurrent.atomic.*
import kotlin.coroutines.*

/**
 * Rendezvous channel is the key synchronization primitive behind coroutines,
 * it is also known as a synchronous queue.
 *
 * At its core, a rendezvous channel is a blocking bounded queue of zero capacity.
 * It supports `send(e)` and `receive()` requests:
 * send(e) checks whether the queue contains any receivers and either removes the first one
 * (i.e. performs a “rendezvous” with it) or adds itself to the queue as a waiting sender and suspends.
 * The `receive()` operation works symmetrically.
 *
 * This implementation never stores `null`-s for simplicity.
 *
 * (You may also follow `SequentialChannelInt` in tests for the sequential semantics).
 */
class RendezvousChannel<E : Any> {
    private val head: AtomicReference<Node<E>>
    private val tail: AtomicReference<Node<E>>

    init {
        val dummy = Dummy<E>()
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    suspend fun send(element: E) {
        while (true) {
            firstReceiverOrNull()?.let {
                it.received.resume(element)
                return@send
            }

            val dispatched = suspendCoroutine { dispatched ->
                val sender = Sender(element, dispatched)
                if (!tryAddNode(sender)) {
                    dispatched.resume(false)
                }
            }

            if (dispatched) {
                break
            }
        }
    }

    suspend fun receive(): E {
        while (true) {
            firstSenderOrNull()?.let {
                it.dispatched.resume(true)
                return@receive it.element as E
            }

            val element = suspendCoroutine { received ->
                val receiver = Receiver(received)
                if (!tryAddNode(receiver)) {
                    received.resume(null)
                }
            }

            if (element != null) {
                return element
            }
        }
    }

    private fun firstReceiverOrNull(): Receiver<E>? {
        val currentHead = head.get()
        val first = currentHead.next.get() ?: return null
        if (first !is Receiver<E>) {
            return null
        }

        return if (head.compareAndSet(currentHead, first)) first else null
    }

    private fun firstSenderOrNull(): Sender<E>? {
        val currentHead = head.get()
        val first = currentHead.next.get() ?: return null
        if (first !is Sender<E>) {
            return null
        }

        return if (head.compareAndSet(currentHead, first)) first else null
    }

    private fun tryAddNode(newNode: Node<E>): Boolean {
        while (true) {
            val currentTail = tail.get()
            val nextTail = tail.get().next.get()

            if (nextTail != null) {
                tail.compareAndSet(currentTail, nextTail)
                continue
            }

            val isEmpty = head.get().next.get() == null
            val mismatch = when (currentTail) {
                is Dummy -> false
                is Receiver -> newNode is Sender
                is Sender -> newNode is Receiver
            }

            if (!isEmpty && mismatch) {
                return false
            }

            if (!currentTail.next.compareAndSet(null, newNode)) {
                continue
            }

            tail.compareAndSet(currentTail, newNode)
            return true
        }
    }

    private sealed class Node<E> {
        val next = AtomicReference<Node<E>?>(null)
    }

    private class Sender<E>(
        val element: E?,
        val dispatched: Continuation<Boolean>
    ): Node<E>()

    private class Receiver<E>(
        val received: Continuation<E>
    ): Node<E>()

    private class Dummy<E> : Node<E>()
}