@file:Suppress("UNCHECKED_CAST")

package day4

import java.util.concurrent.atomic.*
import kotlin.math.absoluteValue

// TODO: WIP
class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) : HashTable<K, V> {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    override fun put(key: K, value: V): V? {
        while (true) {
            // Try to insert the key/value pair.
            val putResult = table.get().put(key, value)
            if (putResult === NEEDS_REHASH) {
                // The current table is too small to insert a new key.
                // Create a new table of x2 capacity,
                // copy all elements to it,
                // and restart the current operation.
                resize()
            } else {
                // The operation has been successfully performed,
                // return the previous value associated with the key.
                return putResult as V?
            }
        }
    }

    override fun get(key: K): V? {
        return table.get().get(key)
    }

    override fun remove(key: K): V? {
        return table.get().remove(key)
    }

    private fun resize() {
        while (true) {
            val current = table.get()
            val next = current.next.get()

            if (next != null) {
                helpResizing(current)
                table.compareAndSet(current, next)
                return
            }

            val newTable = Table<K, V>(current.capacity * 2)
            if (!current.next.compareAndSet(null, newTable)) {
                continue
            }

            helpResizing(current)
            table.compareAndSet(current, newTable)
            break
        }
    }

    private fun helpResizing(current: Table<K,V>) {
        val next = current.next.get() ?: return

        for (index in 0 until current.capacity) {
            while (true) {
                when (val tableKey = current.keys[index]) {
//                    null, is TableItem.Moved -> break
//                    is TableItem.Frozen -> {
//                        next.keys[index] = TableItem.Data(tableKey.value)
//                    }
//                    is TableItem.Data -> {
//                        if (!current.keys.compareAndSet(index, tableKey, TableItem.Frozen(tableKey.value))) {
//                            continue
//                        }
//                    }
//                    is TableItem.Removed<*> -> TODO()
                }
            }
        }
    }

    private class Table<K : Any, V : Any>(val capacity: Int) {
        val next = AtomicReference<Table<K,V>?>(null)
        val keys = AtomicReferenceArray<K?>(capacity)
        val values = AtomicReferenceArray<V?>(capacity)

        fun put(key: K, value: V): Any? {
            // TODO: Copy your implementation from `ConcurrentHashTableWithoutResize`
            // TODO: and add the logic related to moving key/value pairs to a new table.
            var index = index(key)
            // Search for a specified key probing `MAX_PROBES` cells.
            // If neither the key nor an empty cell is found,
            // inform the caller that the table should be resized.
            var probeCount = 0
            while (probeCount < MAX_PROBES) {
                val curKey = keys[index]
                when (curKey) {
                    // The cell contains the specified key.
                    key -> {
                        // Update the value and return the previous one.
                        return values.getAndSet(index, value)
                    }
                    // The cell does not store a key.
                    null -> {
                        // Insert the key/value pair into this cell.
                        if (!keys.compareAndSet(index, null, key)) {
                            continue
                        }
                        if (!values.compareAndSet(index, null, value)) {
                            continue
                        }
                        // No value was associated with the key.
                        return null
                    }
                    // the slot is occupied
                    else -> ++probeCount
                }

                // Process the next cell, use linear probing.
                index = (index + 1) % capacity
            }
            // Inform the caller that the table should be resized.
            return NEEDS_REHASH
        }

        fun get(key: K): V? {
            // TODO: Copy your implementation from `ConcurrentHashTableWithoutResize`
            // TODO: and add the logic related to moving key/value pairs to a new table.
            var index = index(key)
            // Search for a specified key probing `MAX_PROBES` cells.
            // If neither the key is not found after that,
            // the table does not contain it.
            repeat(MAX_PROBES) {
                // Read the key.
                val curKey = keys[index]
                when (curKey) {
                    // The cell contains the required key.
                    key -> {
                        // Read the value associated with the key.
                        return values[index]
                    }
                    // Empty cell.
                    null -> {
                        // The key has not been found.
                        return null
                    }

                    is TableItem.Data<*> -> TODO()
                    is TableItem.Frozen<*> -> TODO()
                    is TableItem.Moved<*> -> TODO()
                    is TableItem.Removed<*> -> TODO()
                }
                // Process the next cell, use linear probing.
                index = (index + 1) % capacity
            }
            // The key has not been found.
            return null
        }

        fun remove(key: K): V? {
            // TODO: Copy your implementation from `ConcurrentHashTableWithoutResize`
            // TODO: and add the logic related to moving key/value pairs to a new table.
            var index = index(key)
            // Search for a specified key probing `MAX_PROBES` cells.
            // If neither the key is not found after that,
            // the table does not contain it.
            repeat(MAX_PROBES) {
                // Read the key.
                val curKey = keys[index]
                when (curKey) {
                    // The cell contains the required key.
                    key -> {
                        // Mark the slot available for `put(..)`,
                        // but do not stop on this cell when searching for a key.
                        // For that, replace the key with `REMOVED_KEY`.
                        // Read the value associated with the key and replace it with `null`.
                        return values.getAndSet(index, null)
                    }
                    // Empty cell.
                    null -> {
                        // The key has not been found.
                        return null
                    }

                    is TableItem.Data<*> -> TODO()
                    is TableItem.Frozen<*> -> TODO()
                    is TableItem.Moved<*> -> TODO()
                    is TableItem.Removed<*> -> TODO()
                }
                // Process the next cell, use linear probing.
                index = (index + 1) % capacity
            }
            // The key has not been found.
            return null
        }

        private fun index(key: Any) = ((key.hashCode() * MAGIC) % capacity).absoluteValue
    }
}

private sealed interface TableItem<T: Any> {
    @JvmInline
    value class Data<T: Any>(val value: T) : TableItem<T>
    @JvmInline
    value class Frozen<T: Any>(val value: T) : TableItem<T>
    @JvmInline
    value class Moved<T: Any>(val value: T) : TableItem<T>
    @JvmInline
    value class Removed<T: Any>(val value: T) : TableItem<T>
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val MAX_PROBES = 2
private val NEEDS_REHASH = Any()