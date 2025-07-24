@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day3

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E {
        // TODO: the cell can store a descriptor
        while (true) {
            when (val value = array.get(index)) {
                is AtomicArrayWithCAS2<*>.CAS2Descriptor -> value.apply()
                is AtomicArrayWithCAS2<*>.DcssDescriptor -> value.complete()
                else -> return value as E
            }
        }
    }

    fun cas2(
        index1: Int, expected1: E, update1: E,
        index2: Int, expected2: E, update2: E
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO: this implementation is not linearizable,
        // TODO: Store a CAS2 descriptor in array[index1].
        val descriptor = if (index1 < index2) {
            CAS2Descriptor(index1, expected1, update1, index2, expected2, update2)
        } else {
            CAS2Descriptor(index2, expected2, update2, index1, expected1, update1)
        }
        descriptor.apply()
        return descriptor.status.get() === CasStatus.SUCCESS
    }

    enum class CasStatus { UNDECIDED, SUCCESS, FAILED }

    inner class CAS2Descriptor(
        val index1: Int,
        val expected1: E,
        val update1: E,
        val index2: Int,
        val expected2: E,
        val update2: E
    ) {
        val status: AtomicReference<Any> = AtomicReference(CasStatus.UNDECIDED)

        fun apply() {
            val success = installDescriptor()
            applyLogically(success)
            applyPhysically()
        }

        private fun installDescriptor() = install(index1, expected1) && install(index2, expected2)

        private fun install(index: Int, expected: E): Boolean {
            while (true) {
                if (status.get() != CasStatus.UNDECIDED) {
                    return false
                }

                when (val currentValue = array[index]) {
                    this -> return true
                    is AtomicArrayWithCAS2<*>.CAS2Descriptor -> currentValue.apply()
                    is AtomicArrayWithCAS2<*>.DcssDescriptor -> currentValue.complete()
                    expected -> {
                        if (dcss(index, currentValue, this, status, CasStatus.UNDECIDED)) {
                            return true
                        }
                    }
                    else -> return false
                }
            }
        }

        private fun applyLogically(success: Boolean) =
            status.compareAndSet(CasStatus.UNDECIDED, if (success) CasStatus.SUCCESS else CasStatus.FAILED)


        private fun applyPhysically() = when (status.get()) {
            CasStatus.SUCCESS -> {
                array.compareAndSet(index1, this, update1)
                array.compareAndSet(index2, this, update2)
            }
            CasStatus.UNDECIDED, CasStatus.FAILED -> {
                array.compareAndSet(index1, this, expected1)
                array.compareAndSet(index2, this, expected2)
            }
            else -> error("Unexpected status: ${status.get()}")
        }
    }

    fun dcss(
        index: Int, expectedArrayValue: Any, newArrayValue: Any,
        reference: AtomicReference<Any>, expectedReferenceValue: Any
    ): Boolean {
        val descriptor = DcssDescriptor(index, expectedArrayValue, newArrayValue, reference, expectedReferenceValue)
        if (!descriptor.install()) {
            return false
        }
        descriptor.complete()
        return descriptor.status.get() == DcssStatus.SUCCESS
    }

    enum class DcssStatus { UNDECIDED, SUCCESS, FAILED }

    private inner class DcssDescriptor(
        val index: Int, val expectedArrayValue: Any, val newArrayValue: Any,
        val reference: AtomicReference<Any>, val expectedReferenceValue: Any
    ) {
        val status = AtomicReference(DcssStatus.UNDECIDED)

        fun install(): Boolean {
            while (true) {
                when (val value = array.get(index)) {
                    this -> return true
                    is AtomicArrayWithCAS2<*>.DcssDescriptor -> value.complete()
                    expectedArrayValue -> {
                        if (array.compareAndSet(index, value, this)) {
                            return true
                        }
                    }
                    else -> return false
                }
            }
        }

        fun complete() {
            while (true) {
                when (status.get()) {
                    DcssStatus.SUCCESS -> {
                        array.compareAndSet(index, this, newArrayValue)
                        return
                    }
                    DcssStatus.FAILED -> {
                        array.compareAndSet(index, this, expectedArrayValue)
                        return
                    }
                    DcssStatus.UNDECIDED -> {
                        if (reference.get() == expectedReferenceValue) {
                            status.compareAndSet(DcssStatus.UNDECIDED, DcssStatus.SUCCESS)
                        } else {
                            status.compareAndSet(DcssStatus.UNDECIDED, DcssStatus.FAILED)
                        }
                    }
                }
            }
        }
    }
}