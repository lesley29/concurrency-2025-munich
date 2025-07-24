@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day3

import day3.AtomicArrayWithCAS2AndImplementedDCSS.Status.*
import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2AndImplementedDCSS<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E {
        // TODO: Copy the implementation from `AtomicArrayWithCAS2Simplified`
        when (val value = array[index]) {
            is AtomicArrayWithCAS2AndImplementedDCSS<*>.CAS2Descriptor -> {
                val previousValue = if (value.index1 == index) value.expected1 else value.expected2
                val newValue = if (value.index1 == index) value.update1 else value.update2

                return when (value.status.get()) {
                    UNDECIDED, FAILED -> previousValue as E
                    SUCCESS -> newValue as E
                    else -> error("Unexpected status: ${value.status.get()}")
                }
            }
            else -> {
                return value as E
            }
        }
    }

    fun cas2(
        index1: Int, expected1: E, update1: E,
        index2: Int, expected2: E, update2: E
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }

        val first = if (index1 < index2) index1 else index2
        val firstExpected = if (first == index1) expected1 else expected2
        val firstUpdate = if (first == index1) update1 else update2

        val second = if (first == index1) index2 else index1
        val secondExpected = if (second == index1) expected1 else expected2
        val secondUpdate = if (second == index1) update1 else update2

        val descriptor = CAS2Descriptor(
            index1 = first, expected1 = firstExpected, update1 = firstUpdate,
            index2 = second, expected2 = secondExpected, update2 = secondUpdate
        )
        descriptor.apply()
        return descriptor.status.get() === SUCCESS
    }

    inner class CAS2Descriptor(
        val index1: Int,
        val expected1: E,
        val update1: E,
        val index2: Int,
        val expected2: E,
        val update2: E
    ) {
        val status = AtomicReference(UNDECIDED)

        fun apply() {
            val success = installDescriptor()
            applyLogically(success)
            applyPhysically()
        }

        private fun installDescriptor(): Boolean {
            // TODO: Install this descriptor to the cells,
            // TODO: returning `true` on success, and `false`
            // TODO: if one of the cells contained an unexpected value.
            return install(index1, expected1) && install(index2, expected2)
        }

        private fun install(index: Int, expected: E): Boolean {
            while (true) {
                if (status.get() != UNDECIDED) {
                    return false
                }

                when (val currentValue = array[index]) {
                    this -> return true
                    is AtomicArrayWithCAS2AndImplementedDCSS<*>.CAS2Descriptor -> currentValue.apply()
                    expected -> {
                        if (dcss(index, currentValue, this, status, UNDECIDED)) {
                            return true
                        }
                    }
                    else -> return false
                }
            }
        }

        private fun applyLogically(success: Boolean) {
            // TODO: Apply this CAS2 operation logically
            // TODO: by updating the descriptor status.
            status.compareAndSet(UNDECIDED, if (success) SUCCESS else FAILED)
        }

        private fun applyPhysically() {
            // TODO: Apply this operation physically
            // TODO: by updating the cells to either
            // TODO: update values (on success)
            // TODO: or back to expected values (on failure).
            when (status.get()) {
                SUCCESS -> {
                    array.compareAndSet(index1, this, update1)
                    array.compareAndSet(index2, this, update2)
                }
                UNDECIDED, FAILED -> {
                    array.compareAndSet(index1, this, expected1)
                    array.compareAndSet(index2, this, expected2)
                }
                else -> error("Unexpected status: ${status.get()}")
            }
        }
    }

    enum class Status {
        UNDECIDED, SUCCESS, FAILED
    }

    // TODO: Please use this DCSS implementation to ensure that
    // TODO: the status is `UNDECIDED` when installing the descriptor.
    fun dcss(
        index: Int,
        expectedCellState: Any?,
        updateCellState: Any?,
        statusReference: AtomicReference<*>,
        expectedStatus: Any?
    ): Boolean =
        if (array[index] == expectedCellState && statusReference.get() == expectedStatus) {
            array[index] = updateCellState
            true
        } else {
            false
        }
}