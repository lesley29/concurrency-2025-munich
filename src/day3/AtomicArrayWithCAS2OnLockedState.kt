package day3

import java.util.concurrent.atomic.*
import java.util.concurrent.locks.*
import kotlin.concurrent.withLock

// This implementation never stores `null` values.
class AtomicArrayWithCAS2OnLockedState<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E {
        // TODO: Cover the case when the cell state is LOCKED.
        while (true) {
            val value = array[index]
            if (value != LOCKED) {
                @Suppress("UNCHECKED_CAST")
                return value as E
            }
        }
    }

//    fun cas2(
//        index1: Int, expected1: E, update1: E,
//        index2: Int, expected2: E, update2: E
//    ): Boolean {
//        require(index1 != index2) { "The indices should be different" }
//        // TODO: Make me thread-safe by "locking" the cells
//        // TODO: via atomically changing their states to LOCKED.
//        val first = if (index1 < index2) index1 else index2
//        val firstExpected = if (first == index1) expected1 else expected2
//        val firstUpdate = if (first == index1) update1 else update2
//
//        val second = if (first == index1) index2 else index1
//        val secondExpected = if (second == index1) expected1 else expected2
//        val secondUpdate = if (second == index1) update1 else update2
//
//        while (true) {
//            if (array[first] == LOCKED) {
//                continue
//            }
//
//            if (!array.compareAndSet(first, firstExpected, LOCKED)) {
//                return false
//            }
//
//            while (true) {
//                if (array[second] == LOCKED) {
//                    continue
//                }
//
//                if (!array.compareAndSet(second, secondExpected, LOCKED)) {
//                    array.compareAndSet(first, LOCKED, firstExpected)
//                    return false
//                }
//
//                break
//            }
//
//            array.set(first, firstUpdate)
//            array.set(second, secondUpdate)
//            return true
//        }
//    }

    fun cas2(
        index1: Int, expected1: E, update1: E,
        index2: Int, expected2: E, update2: E
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO: Make me thread-safe by "locking" the cells
        // TODO: via atomically changing their states to LOCKED.
        val first = if (index1 < index2) index1 else index2
        val firstExpected = if (first == index1) expected1 else expected2
        val firstUpdate = if (first == index1) update1 else update2

        val second = if (first == index1) index2 else index1
        val secondExpected = if (second == index1) expected1 else expected2
        val secondUpdate = if (second == index1) update1 else update2

        while (true) {
            if (!lockCell(first, firstExpected)) {
                return false
            }

            if (!lockCell(second, secondExpected)) {
                array.compareAndSet(first, LOCKED, firstExpected)
                return false
            }

            array.set(first, firstUpdate)
            array.set(second, secondUpdate)
            return true
        }
    }

    private fun lockCell(index: Int, expectedValue: E): Boolean {
        while (true) {
            when (array[index]) {
                expectedValue -> {
                    if (array.compareAndSet(index, expectedValue, LOCKED)) {
                        return true
                    }
                }
                LOCKED -> continue
                else -> return false
            }
        }
    }
}

// TODO: Store me in `a` to indicate that the reference is "locked".
// TODO: Other operations should wait in an active loop until the
// TODO: value changes.
private val LOCKED = "Locked"