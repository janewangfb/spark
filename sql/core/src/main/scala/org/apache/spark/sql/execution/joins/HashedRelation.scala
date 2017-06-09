/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import java.io._

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.memory.{MemoryConsumer, StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.{Platform, UnsafeAlignedOffset}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Convenient class for returned values from map iterator
 */
private[execution] case class Item(page: Object, offset: Long, row: InternalRow)

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation extends KnownSizeEstimation {
  /**
   * Returns matched rows.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: InternalRow): Iterator[Item]

  /**
   * Returns matched rows for a key that has only one column with LongType.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: Long): Iterator[Item] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the matched single row.
   */
  def getValue(key: InternalRow): InternalRow

  /**
   * Returns the matched single row with key that have only one column of LongType.
   */
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns whether the hash relation is built on the outer join side
   */
  def isOuterJoinHashSameSide() : Boolean

  /**
   * Return those rows that do not have matches in the other table
   */
  def getUnMatched() : Iterator[InternalRow]

  /**
   * Mark a row as matched with some rows in the other table
   */
  def markMatched(item: Item) : Unit

  /**
   * Return those rows with null keys.
   */
  def getNullRows(): Iterator[InternalRow]

  /**
   * Returns true iff all the keys are unique.
   */
  def keyIsUnique: Boolean

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  def asReadOnlyCopy(): HashedRelation

  /**
   * Release any used resources.
   */
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   */
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      outerJoinHashSameSide: Boolean = false,
      sizeEstimate: Int = 64,
      taskMemoryManager: TaskMemoryManager = null): HashedRelation = {
    val mm = Option(taskMemoryManager).getOrElse {
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0)
    }

    if (key.length == 1 && key.head.dataType == LongType) {
      LongHashedRelation(input, key, outerJoinHashSameSide, sizeEstimate, mm)
    } else {
      UnsafeHashedRelation(input, key, outerJoinHashSameSide, sizeEstimate, mm)
    }
  }
}

private[joins] class BytesToUnsafeRowMap(taskMemoryManager: TaskMemoryManager,
                                        initialCapacity: Int,
                                        pageSizeBytes: Long)
  extends BytesToBytesMap(taskMemoryManager, initialCapacity, pageSizeBytes) {

  private val PAGE_NUMBER_BITS = TaskMemoryManager.PAGE_NUMBER_BITS
  private val MATCHED_BITS = 1
  private val OFFSET_BITS = TaskMemoryManager.OFFSET_BITS - MATCHED_BITS

  private val PAGE_NUMBER_MASK = 0x1FFFFL
  private val MATCHED_MASK = 0x1L
  private val OFFSET_MASK = 0x3FFFFFFFFFFFFL

  private[joins] def toAddress(page: Int, matched: Int, offset: Long): Long = {
    page.toLong << (OFFSET_BITS + MATCHED_BITS) | matched.toLong << OFFSET_BITS | offset
  }

  private[joins] def toPage(address: Long): Int = {
    ((address >>> (OFFSET_BITS + MATCHED_BITS)) & PAGE_NUMBER_MASK).toInt
  }

  private[joins] def toOffset(address: Long): Long = {
    taskMemoryManager.getOffsetInPage(toAddress(toPage(address), 0, address & OFFSET_MASK))
  }

  private[joins] def toMatched(address: Long): Int = {
    ((address >>> OFFSET_BITS) & MATCHED_MASK).toInt
  }

  override def unMaskMatchedBits(fullKeyAddress: Long): Long = {
    fullKeyAddress & 0xFFFBFFFFFFFFFFFFL
  }

  def markMatched(item: Item): Unit = {
    if (item.page == Nil) {
      val pos = item.offset.toInt
      val address = longArray.get(pos)
      longArray.set(pos, toAddress(toPage(address), 1, address & OFFSET_MASK))
    } else {
      val address = Platform.getLong(item.page, item.offset)
      val a = toAddress(toPage(address), 1, address & OFFSET_MASK)
      Platform.putLong(item.page, item.offset, a)
    }
  }

  /**
   * Returns an iterator of UnsafeRow for multiple linked values.
   */
  def valueIter(pos: Int, resultRow: UnsafeRow): Iterator[Item] = {
    new Iterator[Item] {
      val uaoSize = UnsafeAlignedOffset.getUaoSize
      var page: Object = Nil
      var offset: Long = pos
      var addr = longArray.get(pos)
      override def hasNext: Boolean = addr != 0
      override def next(): Item = {
        var recordPage = taskMemoryManager.getPage(addr)
        var recordMatched = toMatched(addr)
        var recordOffset = toOffset(addr)

        val recordLength = UnsafeAlignedOffset.getSize(recordPage, recordOffset)
        recordOffset += uaoSize
        val keyLength = UnsafeAlignedOffset.getSize(recordPage, recordOffset)
        recordOffset += uaoSize + keyLength
        val valueLength = recordLength - keyLength - uaoSize
        resultRow.pointTo(recordPage, recordOffset, valueLength)
        val result = Item(page, offset, resultRow)

        recordOffset += valueLength
        addr = Platform.getLong(recordPage, recordOffset)
        page = recordPage
        offset = recordOffset

        result
      }
    }
  }

  def getUnMatched(resultRow: UnsafeRow) : Iterator[InternalRow] = {
    new Iterator[UnsafeRow] {
      var i = -2
      var iter : Iterator[Item] = Iterator.empty

      override def hasNext: Boolean = {
        if (!iter.isEmpty) {
          while (iter.hasNext) {
            val item = iter.next()
            val address =
              if (item.page == Nil) {
                longArray.get(item.offset.toInt)
              } else {
                Platform.getLong(item.page, item.offset)
              }
            if (toMatched(address) == 0) {
              return true
            }
          }
        }

        i += 2
        while (i < longArray.size - 1) {
          if (longArray.get(i) != 0) {
            iter = valueIter(i, resultRow)
            while (iter.hasNext) {
              val item = iter.next()
              val address =
                if (item.page == Nil) {
                  longArray.get(item.offset.toInt)
                } else {
                  Platform.getLong(item.page, item.offset)
                }
              if (toMatched(address) == 0) {
                return true;
              }
            }
          }
          i += 2
        }
        false
      }

      override def next(): UnsafeRow = {
        resultRow
      }
    }
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of value] [key bytes] [bytes for value]
 */
private[joins] class UnsafeHashedRelation(
    private var numFields: Int,
    private var binaryMap: BytesToUnsafeRowMap,
    private var nullRows: Seq[UnsafeRow] = Nil,
    private var outerJoinHashSameSide: Boolean = false)
  extends HashedRelation with Externalizable with KryoSerializable {

  private[joins] def this() = this(0, null)  // Needed for serialization

  override def keyIsUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def asReadOnlyCopy(): UnsafeHashedRelation = {
    new UnsafeHashedRelation(numFields, binaryMap, nullRows, outerJoinHashSameSide)
  }

  override def estimatedSize: Long = binaryMap.getTotalMemoryConsumption

  // re-used in get()/getValue()
  var resultRow = new UnsafeRow(numFields)

  override def get(key: InternalRow): Iterator[Item] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      new Iterator[Item] {
        private var _hasNext = true
        private var offset: Long = loc.getPos * 2
        private var page: Object = Nil
        override def hasNext: Boolean = _hasNext
        override def next(): Item = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          val result = Item(page, offset, resultRow)
          page = loc.getKeyBase
          offset = loc.getValueOffset + loc.getValueLength
          _hasNext = loc.nextValue()
          result
        }
      }
    } else {
      null
    }
  }

  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

  override def isOuterJoinHashSameSide() : Boolean = outerJoinHashSameSide

  def getUnMatched() : Iterator[InternalRow] = {
    if (outerJoinHashSameSide) {
      binaryMap.asInstanceOf[BytesToUnsafeRowMap].getUnMatched(resultRow)
    } else {
      Iterator.empty
    }
  }

  override def markMatched(item: Item): Unit = {
    if (outerJoinHashSameSide) {
      binaryMap.asInstanceOf[BytesToUnsafeRowMap].markMatched(item)
    }
  }

  override def getNullRows(): Iterator[InternalRow] = {
    if (outerJoinHashSameSide) {
      nullRows.iterator
    } else {
      Iterator.empty
    }
  }

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeBoolean, out.writeLong, out.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeBoolean, out.writeLong, out.write)
  }

  private def write(
      writeInt: (Int) => Unit,
      writeBoolean: (Boolean) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit) : Unit = {
    writeInt(numFields)
    writeBoolean(outerJoinHashSameSide)
    // TODO: move these into BytesToBytesMap
    writeLong(binaryMap.numKeys())
    writeLong(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      writeBuffer(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      writeInt(loc.getKeyLength)
      writeInt(loc.getValueLength)

      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }

    if (outerJoinHashSameSide) {
      // write out null rows
      writeInt(nullRows.size)
      val nullIter = nullRows.iterator
      while (nullIter.hasNext) {
        val row = nullIter.next()
        writeInt(row.getSizeInBytes)
        write(row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
      }

      // write out the matched info
      var i = 0
      var array = binaryMap.getArray
      var map = binaryMap.asInstanceOf[BytesToUnsafeRowMap]
      while (i < array.size - 1) {
        val iter = map.valueIter(i, resultRow)
        while (iter.hasNext) {
          val item = iter.next()
          val address =
            if (item.page == Nil) {
              array.get(item.offset.toInt)
            } else {
              Platform.getLong(item.page, item.offset)
            }
          writeInt(map.toMatched(address))
        }
        i += 2
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(in.readInt, in.readBoolean, in.readLong, in.readFully)
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(in.readInt, in.readBoolean, in.readLong, in.readBytes)
  }

  private def read(
      readInt: () => Int,
      readBoolean: () => Boolean,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    numFields = readInt()
    outerJoinHashSameSide = readBoolean()
    resultRow = new UnsafeRow(numFields)
    val nKeys = readLong()
    val nValues = readLong()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // TODO(josh): We won't need this dummy memory manager after future refactorings; revisit
    // during code review

    binaryMap =
      new BytesToUnsafeRowMap(taskMemoryManager,
        (nKeys * 1.5 + 1).toInt, // reduce hash collision
        pageSizeBytes)


    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nValues) {
      val keySize = readInt()
      val valuesSize = readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      readBuffer(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      readBuffer(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSuceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        binaryMap.free()
        throw new IOException("Could not allocate memory to grow BytesToUnsafeRowMap")
      }
      i += 1
    }

    if (outerJoinHashSameSide) {
      // read null rows
      val nNullRows = readInt()
      nullRows = Nil
      var i = 0;
      while (i < nNullRows) {
        val rowSize = readInt()
        var rowBuffer = new Array[Byte](rowSize)
        readBuffer(rowBuffer, 0, rowSize)
        val nullRow = new UnsafeRow(numFields)
        nullRow.pointTo(rowBuffer, rowSize)
        nullRows = nullRows :+ (nullRow)
        i += 1
      }

      // set the matched info
      i = 0
      var array = binaryMap.getArray
      var map = binaryMap.asInstanceOf[BytesToUnsafeRowMap]
      while (i < array.size - 1) {
        val iter = map.valueIter(i, resultRow)
        while (iter.hasNext) {
          val item = iter.next()
          if (readInt() != 0) {
            map.markMatched(item)
          }
        }
        i += 2
      }
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      outerJoinHashSameSide: Boolean,
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): HashedRelation = {
    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    val binaryMap =
      new BytesToUnsafeRowMap(
        taskMemoryManager,
        // Only 70% of the slots can be used before growing,
        // more capacity help to reduce collision
        (sizeEstimate * 1.5 + 1).toInt,
        pageSizeBytes)

    var nullRows : Seq[UnsafeRow] = Nil

    // Create a mapping of buildKeys -> rows
    val keyGenerator = UnsafeProjection.create(key)
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeRow]
      val b = row.isNullAt(0)
      numFields = row.numFields()
      val key = keyGenerator(row)
      if (key.anyNull) {
        if (outerJoinHashSameSide) {
          nullRows = nullRows :+ (row)
        }
      } else {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
        if (!success) {
          binaryMap.free()
          throw new SparkException("There is no enough memory to build hash map")
        }
      }
    }

    new UnsafeHashedRelation(numFields, binaryMap, nullRows, outerJoinHashSameSide)
  }
}

/**
 * An append-only hash map mapping from key of Long to UnsafeRow.
 *
 * The underlying bytes of all values (UnsafeRows) are packed together as a single byte array
 * (`page`) in this format:
 *
 *  [bytes of row1][address1][bytes of row2][address1] ...
 *
 *  address1 (8 bytes) is the offset and size of next value for the same key as row1, any key
 *  could have multiple values. the address at the end of last value for every key is 0.
 *
 * The keys and addresses of their values could be stored in two modes:
 *
 * 1) sparse mode: the keys and addresses are stored in `array` as:
 *
 *  [key1][address1][key2][address2]...[]
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1. The position of key1
 *  is determined by `key1 % cap`. Quadratic probing with triangular numbers is used to address
 *  hash collision.
 *
 * 2) dense mode: all the addresses are packed into a single array of long, as:
 *
 *  [address1] [address2] ...
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1, the position is
 *  determined by `key1 - minKey`.
 *
 * The map is created as sparse mode, then key-value could be appended into it. Once finish
 * appending, caller could all optimize() to try to turn the map into dense mode, which is faster
 * to probe.
 *
 * see http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
 */
private[execution] final class LongToUnsafeRowMap(val mm: TaskMemoryManager, capacity: Int)
  extends MemoryConsumer(mm) with Externalizable with KryoSerializable {

  // Whether the keys are stored in dense mode or not.
  private var isDense = false

  // The minimum key
  private var minKey = Long.MaxValue

  // The maxinum key
  private var maxKey = Long.MinValue

  // The array to store the key and offset of UnsafeRow in the page.
  //
  // Sparse mode: [key1] [offset1 | size1] [key2] [offset | size2] ...
  // Dense mode: [offset1 | size1] [offset2 | size2]
  private var array: Array[Long] = null
  private var mask: Int = 0

  // The page to store all bytes of UnsafeRow and the pointer to next rows.
  // [row1][pointer1] [row2][pointer2]
  private var page: Array[Long] = null

  // Current write cursor in the page.
  private var cursor: Long = Platform.LONG_ARRAY_OFFSET

  // The number of bits for size in address
  private val SIZE_BITS = 28
  private val SIZE_MASK = 0xfffffff

  // The number of bits for matched row indicator
  private val MATCHED_BITS = 1
  private val MATCHED_MASK = 0x1

  // The total number of values of all keys.
  private var numValues = 0L

  // The number of unique keys.
  private var numKeys = 0L

  // needed by serializer
  def this() = {
    this(
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0),
      0)
  }

  private def ensureAcquireMemory(size: Long): Unit = {
    // do not support spilling
    val got = acquireMemory(size)
    if (got < size) {
      freeMemory(got)
      throw new SparkException(s"Can't acquire $size bytes memory to build hash relation, " +
        s"got $got bytes")
    }
  }

  private def init(): Unit = {
    if (mm != null) {
      require(capacity < 512000000, "Cannot broadcast more than 512 millions rows")
      var n = 1
      while (n < capacity) n *= 2
      ensureAcquireMemory(n * 2L * 8 + (1 << 20))
      array = new Array[Long](n * 2)
      mask = n * 2 - 2
      page = new Array[Long](1 << 17)  // 1M bytes
    }
  }

  init()

  def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Returns whether all the keys are unique.
   */
  def keyIsUnique: Boolean = numKeys == numValues

  /**
   * Returns total memory consumption.
   */
  def getTotalMemoryConsumption: Long = array.length * 8L + page.length * 8L

  /**
   * Returns the first slot of array that store the keys (sparse mode).
   */
  private def firstSlot(key: Long): Int = {
    val h = key * 0x9E3779B9L
    (h ^ (h >> 32)).toInt & mask
  }

  /**
   * Returns the next probe in the array.
   */
  private def nextSlot(pos: Int): Int = (pos + 2) & mask

  private[this] def toAddress(offset: Long, matchedBit: Int, size: Int): Long = {
    ((offset - Platform.LONG_ARRAY_OFFSET) << (SIZE_BITS + MATCHED_BITS)) |
      matchedBit << SIZE_BITS | size
  }

  private[this] def toOffset(address: Long): Long = {
    (address >>> (SIZE_BITS + MATCHED_BITS)) + Platform.LONG_ARRAY_OFFSET
  }

  private[this] def toSize(address: Long): Int = {
    (address & SIZE_MASK).toInt
  }

  private[this] def toMatched(address: Long): Int = {
    ((address >>> SIZE_BITS) & MATCHED_MASK).toInt
  }

  private def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(page, toOffset(address), toSize(address))
    resultRow
  }

  /**
   * Returns the single UnsafeRow for given key, or null if not found.
   */
  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return getRow(value, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return getRow(array(pos + 1), resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Returns an iterator of UnsafeRow for multiple linked values.
   */
  private def valueIter(pos: Int, resultRow: UnsafeRow)
    : Iterator[Item] = {
    new Iterator[Item] {
      var addr = array(pos)
      var baseObject: Object = Nil
      var offset: Long = pos
      override def hasNext: Boolean = addr != 0
      override def next(): Item = {
        val pageOffset = toOffset(addr)
        val size = toSize(addr)
        resultRow.pointTo(page, pageOffset, size)
        val result = Item(baseObject, offset, resultRow)
        offset = pageOffset + size
        baseObject = page
        addr = Platform.getLong(page, pageOffset + size)
        result
      }
    }
  }

  /**
   * Returns an iterator for all the values for the given key, or null if no value found.
   */
  def get(key: Long, resultRow: UnsafeRow): Iterator[Item] = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val idx = (key - minKey).toInt
        val value = array(idx)
        if (value > 0) {
          return valueIter(idx, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return valueIter(pos + 1, resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Appends the key and row into this map.
   */
  def append(key: Long, row: UnsafeRow): Unit = {
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << SIZE_BITS)) {
      sys.error("Does not support row that is larger than 256M")
    }

    if (key < minKey) {
      minKey = key
    }
    if (key > maxKey) {
      maxKey = key
    }

    // There is 8 bytes for the pointer to next value
    if (cursor + 8 + row.getSizeInBytes > page.length * 8L + Platform.LONG_ARRAY_OFFSET) {
      val used = page.length
      if (used >= (1 << 30)) {
        sys.error("Can not build a HashedRelation that is larger than 8G")
      }
      ensureAcquireMemory(used * 8L * 2)
      val newPage = new Array[Long](used * 2)
      Platform.copyMemory(page, Platform.LONG_ARRAY_OFFSET, newPage, Platform.LONG_ARRAY_OFFSET,
        cursor - Platform.LONG_ARRAY_OFFSET)
      page = newPage
      freeMemory(used * 8L)
    }

    // copy the bytes of UnsafeRow
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)
    cursor += row.getSizeInBytes
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1
    updateIndex(key, toAddress(offset, 0, row.getSizeInBytes))
  }

  /**
   * Update the address in array for given key.
   */
  private def updateIndex(key: Long, address: Long): Unit = {
    var pos = firstSlot(key)
    assert(numKeys < array.length / 2)
    while (array(pos) != key && array(pos + 1) != 0) {
      pos = nextSlot(pos)
    }
    if (array(pos + 1) == 0) {
      // this is the first value for this key, put the address in array.
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 4 > array.length) {
        // reach half of the capacity
        if (array.length < (1 << 30)) {
          // Cannot allocate an array with 2G elements
          growArray()
        } else if (numKeys > array.length / 2 * 0.75) {
          // The fill ratio should be less than 0.75
          sys.error("Cannot build HashedRelation with more than 1/3 billions unique keys")
        }
      }
    } else {
      // there are some values for this key, put the address in the front of them.
      val pointer = toOffset(address) + toSize(address)
      Platform.putLong(page, pointer, array(pos + 1))
      array(pos + 1) = address
    }
  }

  private def growArray(): Unit = {
    var old_array = array
    val n = array.length
    numKeys = 0
    ensureAcquireMemory(n * 2 * 8L)
    array = new Array[Long](n * 2)
    mask = n * 2 - 2
    var i = 0
    while (i < old_array.length) {
      if (old_array(i + 1) > 0) {
        updateIndex(old_array(i), old_array(i + 1))
      }
      i += 2
    }
    old_array = null  // release the reference to old array
    freeMemory(n * 8L)
  }

  def getUnMatched(resultRow: UnsafeRow) : Iterator[InternalRow] = {
    // scalastyle:off println
    println("!!!!!!!!! map size: " + array.length)
    // scalastyle:on
    new Iterator[UnsafeRow] {
      val idxIncr = if (isDense) 1 else 2
      val addrAdjust = if (isDense) 0 else 1
      var i = if (isDense) -1 else -2
      var iter : Iterator[Item] = Iterator.empty

      override def hasNext: Boolean = {
        if (iter != null) {
          while (iter.hasNext) {
            val item = iter.next()
            val address =
              if (item.page == Nil) {
                array(item.offset.toInt)
              } else {
                Platform.getLong(item.page, item.offset)
              }
            if (toMatched(address) == 0) {
              return true
            }
          }
        }

        i += idxIncr
        while (i < array.length) {
          if (array(i + addrAdjust) != 0) {
            iter = valueIter(i + addrAdjust, resultRow)
            while (iter.hasNext) {
              val item = iter.next()
              val address =
                if (item.page == Nil) {
                  array(item.offset.toInt)
                } else {
                  Platform.getLong(item.page, item.offset)
                }
              if (toMatched(address) == 0) {
                return true;
              }
            }
          }
          i += idxIncr
        }
        false
      }

      override def next(): UnsafeRow = {
        resultRow
      }
    }
  }

  def markMatched(item: Item) : Unit = {
    if (item.page == Nil) {
      val offset2 = item.offset.toInt
      val address = array(offset2)
      array(offset2) = toAddress(toOffset(address), 1, toSize(address))
    } else {
      val address = Platform.getLong(page, item.offset)
      val a = toAddress(toOffset(address), 1, toSize(address))
      Platform.putLong(page, item.offset, a)
    }
  }

  /**
   * Try to turn the map into dense mode, which is faster to probe.
   */
  def optimize(): Unit = {
    val range = maxKey - minKey
    // Convert to dense mode if it does not require more memory or could fit within L1 cache
    // SPARK-16740: Make sure range doesn't overflow if minKey has a large negative value
    if (range >= 0 && (range < array.length || range < 1024)) {
      try {
        ensureAcquireMemory((range + 1) * 8L)
      } catch {
        case e: SparkException =>
          // there is no enough memory to convert
          return
      }
      val denseArray = new Array[Long]((range + 1).toInt)
      var i = 0
      while (i < array.length) {
        if (array(i + 1) > 0) {
          val idx = (array(i) - minKey).toInt
          denseArray(idx) = array(i + 1)
        }
        i += 2
      }
      val old_length = array.length
      array = denseArray
      isDense = true
      freeMemory(old_length * 8L)
    }
  }

  /**
   * Free all the memory acquired by this map.
   */
  def free(): Unit = {
    if (page != null) {
      freeMemory(page.length * 8L)
      page = null
    }
    if (array != null) {
      freeMemory(array.length * 8L)
      array = null
    }
  }

  private def writeLongArray(
      writeBuffer: (Array[Byte], Int, Int) => Unit,
      arr: Array[Long],
      len: Int): Unit = {
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = len * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      Platform.copyMemory(arr, offset, buffer, Platform.BYTE_ARRAY_OFFSET, size)
      writeBuffer(buffer, 0, size.toInt)
      offset += size
    }
  }

  private def write(
      writeBoolean: (Boolean) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    writeBoolean(isDense)
    writeLong(minKey)
    writeLong(maxKey)
    writeLong(numKeys)
    writeLong(numValues)

    writeLong(array.length)
    writeLongArray(writeBuffer, array, array.length)
    val used = ((cursor - Platform.LONG_ARRAY_OFFSET) / 8).toInt
    writeLong(used)
    writeLongArray(writeBuffer, page, used)
  }

  override def writeExternal(output: ObjectOutput): Unit = {
    write(output.writeBoolean, output.writeLong, output.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    write(out.writeBoolean, out.writeLong, out.write)
  }

  private def readLongArray(
      readBuffer: (Array[Byte], Int, Int) => Unit,
      length: Int): Array[Long] = {
    val array = new Array[Long](length)
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = length * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      readBuffer(buffer, 0, size.toInt)
      Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, array, offset, size)
      offset += size
    }
    array
  }

  private def read(
      readBoolean: () => Boolean,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    isDense = readBoolean()
    minKey = readLong()
    maxKey = readLong()
    numKeys = readLong()
    numValues = readLong()

    val length = readLong().toInt
    mask = length - 2
    array = readLongArray(readBuffer, length)
    val pageLength = readLong().toInt
    page = readLongArray(readBuffer, pageLength)
  }

  override def readExternal(in: ObjectInput): Unit = {
    read(in.readBoolean, in.readLong, in.readFully)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    read(in.readBoolean, in.readLong, in.readBytes)
  }
}

private[joins] class LongHashedRelation(
    private var nFields: Int,
    private var map: LongToUnsafeRowMap,
    private var nullRows: Seq[UnsafeRow] = Nil,
    private var outerJoinHashSameSide: Boolean = false) extends HashedRelation with Externalizable {

  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def asReadOnlyCopy(): LongHashedRelation =
    new LongHashedRelation(nFields, map, nullRows, outerJoinHashSameSide)

  override def estimatedSize: Long = map.getTotalMemoryConsumption

  override def get(key: InternalRow): Iterator[Item] = {
    if (key.isNullAt(0)) {
      null
    } else {
      get(key.getLong(0))
    }
  }

  override def getValue(key: InternalRow): InternalRow = {
    if (key.isNullAt(0)) {
      null
    } else {
      getValue(key.getLong(0))
    }
  }

  override def get(key: Long): Iterator[Item] = map.get(key, resultRow)

  override def getValue(key: Long): InternalRow = map.getValue(key, resultRow)

  override def keyIsUnique: Boolean = map.keyIsUnique

  override def isOuterJoinHashSameSide() : Boolean = outerJoinHashSameSide

  override def getUnMatched(): Iterator[InternalRow] = {
    if (outerJoinHashSameSide) {
      map.getUnMatched(resultRow)
    } else {
      Iterator.empty
    }
    map.getUnMatched(resultRow)
  }

  override def markMatched(item: Item): Unit = {
    if (outerJoinHashSameSide) {
      map.markMatched(item)
    }
  }

  override def getNullRows(): Iterator[InternalRow] = {
    if (outerJoinHashSameSide) {
      nullRows.iterator
    } else {
      Iterator.empty
    }
  }

  override def close(): Unit = {
    map.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeBoolean(outerJoinHashSameSide)
    out.writeObject(map)

    if (outerJoinHashSameSide) {
      var buffer = new Array[Byte](64)
      def write(base: Object, offset: Long, length: Int): Unit = {
        if (buffer.length < length) {
          buffer = new Array[Byte](length)
        }
        Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
        out.write(buffer, 0, length)
      }

      // write out null rows
      out.writeInt(nullRows.size)
      val nullIter = nullRows.iterator
      while (nullIter.hasNext) {
        val row = nullIter.next()
        out.writeInt(row.getSizeInBytes)
        write(row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    outerJoinHashSameSide = in.readBoolean()
    resultRow = new UnsafeRow(nFields)
    map = in.readObject().asInstanceOf[LongToUnsafeRowMap]

    if (outerJoinHashSameSide) {
      // read null rows
      val nNullRows = in.readInt()
      nullRows = Nil
      var i = 0;
      while (i < nNullRows) {
        val rowSize = in.readInt()

        var rowBuffer = new Array[Byte](rowSize)
        in.read(rowBuffer, 0, rowSize)
        val nullRow = new UnsafeRow(nFields)
        nullRow.pointTo(rowBuffer, rowSize)
        nullRows = nullRows :+ (nullRow)
        i += 1
      }
    }
  }
}

/**
 * Create hashed relation with key that is long.
 */
private[joins] object LongHashedRelation {
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      outerJoinHashSameSide: Boolean,
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): LongHashedRelation = {

    val map = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate)
    val keyGenerator = UnsafeProjection.create(key)

    var nullRows : Seq[UnsafeRow] = Nil

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (rowKey.isNullAt(0)) {
        if (outerJoinHashSameSide) nullRows = nullRows :+ (unsafeRow)
      } else {
        val key = rowKey.getLong(0)
        map.append(key, unsafeRow)
      }
    }
    map.optimize()
    new LongHashedRelation(numFields, map, nullRows, outerJoinHashSameSide)
  }
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
private[execution] case class HashedRelationBroadcastMode(key: Seq[Expression])
  extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    HashedRelation(rows.iterator, canonicalized.key, false, rows.length)
  }

  override lazy val canonicalized: HashedRelationBroadcastMode = {
    this.copy(key = key.map(_.canonicalized))
  }
}
