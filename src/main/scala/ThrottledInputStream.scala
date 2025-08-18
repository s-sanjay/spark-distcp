/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.InputStream
import java.io.IOException
import org.apache.hadoop.fs.Seekable

/**
 * The ThrottleInputStream provides bandwidth throttling on a specified
 * InputStream. It is implemented as a wrapper on top of another InputStream
 * instance.
 * The throttling works by examining the number of bytes read from the underlying
 * InputStream from the beginning, and sleep()ing for a time interval if
 * the byte-transfer is found exceed the specified tolerable maximum.
 * (Thus, while the read-rate might exceed the maximum for a given short interval,
 * the average tends towards the specified maximum, overall.)
 */
class ThrottledInputStream(rawStream: InputStream, maxBytesPerSec: Float) extends InputStream with Seekable
  with AutoCloseable {
  require(maxBytesPerSec > 0, s"Bandwidth $maxBytesPerSec is invalid")

  private val startTime = System.currentTimeMillis()
  private var bytesRead = 0L
  private var totalSleepTime = 0L

  private val SLEEP_DURATION_MS = 50L

  def this(rawStream: InputStream) = this(rawStream, Long.MaxValue)

  override def close(): Unit = rawStream.close()

  override def read(): Int = {
    throttle()
    val data: Int = rawStream.read()
    if (data != -1) bytesRead += 1
    data
  }

  override def read(b: Array[Byte]): Int = {
    throttle()
    val readLen = rawStream.read(b)
    if (readLen != -1) bytesRead += readLen
    readLen
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (len == 0) return 0
    throttle()
    val readLen = rawStream.read(b, off, len)
    if (readLen != -1) bytesRead += readLen
    readLen
  }

  private def throttle(): Unit = {
    while (getBytesPerSec > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS)
        totalSleepTime += SLEEP_DURATION_MS
      } catch {
        case e: InterruptedException =>
          throw new IOException("Thread aborted", e)
      }
    }
  }

  /**
   * Getter for the read-rate from this stream, since creation.
   * Calculated as bytesRead/elapsedTimeSinceStart.
   *
   * @return Read rate, in bytes/sec.
   */
  def getBytesPerSec: Long = {
    val elapsed = (System.currentTimeMillis() - startTime) / 1000
    if (elapsed == 0) bytesRead else bytesRead / elapsed
  }

  /**
   * Getter the total time spent in sleep.
   *
   * @return Number of milliseconds spent in sleep.
   */
  def getTotalSleepTime: Long = totalSleepTime

  def getBytesRead: Long = bytesRead

  override def toString: String =
    s"ThrottledInputStream(bytesRead=$getBytesRead, maxBytesPerSec=$maxBytesPerSec, " +
      s"bytesPerSec=$getBytesPerSec, totalSleepTime=$getTotalSleepTime)"

  private def checkSeekable(): Unit = {
    if (!rawStream.isInstanceOf[Seekable]) {
      throw new UnsupportedOperationException("seek operations are unsupported by the internal stream")
    }
  }

  override def seek(pos: Long): Unit = {
    checkSeekable()
    rawStream.asInstanceOf[Seekable].seek(pos)
  }

  override def getPos: Long = {
    checkSeekable()
    rawStream.asInstanceOf[Seekable].getPos
  }

  override def seekToNewSource(targetPos: Long): Boolean = {
    checkSeekable()
    rawStream.asInstanceOf[Seekable].seekToNewSource(targetPos)
  }
}
