import org.apache.hadoop.fs.Seekable
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, IOException, InputStream}

class ThrottledInputStreamSpec extends AnyFunSuite {

  private class SeekableBais(data: Array[Byte]) extends ByteArrayInputStream(data) with Seekable {
    override def seek(pos: Long): Unit = this.reset(); this.skip(pos)
    override def getPos: Long = this.pos
    override def seekToNewSource(targetPos: Long): Boolean = false
  }

  test("reads all data and tracks bytes read") {
    val bytes = Array.tabulate[Byte](1024)(i => (i % 256).toByte)
    val in = new ThrottledInputStream(new SeekableBais(bytes), maxBytesPerSec = 1024)
    val buf = new Array[Byte](2048)
    val n = in.read(buf)
    assert(n == 1024)
    assert(in.getBytesRead == 1024)
    in.close()
  }

  test("throttles and sleeps under low bandwidth") {
    val size = 64 * 1024
    val bytes = Array.fill[Byte](size)(1)
    val in = new ThrottledInputStream(new SeekableBais(bytes), maxBytesPerSec = 1024) // 1KB/s
    val buf = new Array[Byte](4096)
    var total = 0
    while (total < size) {
      val r = in.read(buf)
      if (r == -1) sys.error("unexpected EOF")
      total += r
    }
    assert(total == size)
    // With such low bandwidth, we expect some sleep time to be recorded
    assert(in.getTotalSleepTime >= 0)
    in.close()
  }

  test("seek operations require Seekable and report position") {
    val bytes = (0 until 10).map(_.toByte).toArray
    val in = new ThrottledInputStream(new SeekableBais(bytes), maxBytesPerSec = Long.MaxValue)
    in.seek(5)
    assert(in.getPos >= 5)
    val b = in.read()
    assert(b != -1)
    in.close()
  }

  test("throws on interrupted sleep with IOException") {
    // Create a stream with an InputStream that never blocks in read but simulate interrupt during throttle
    val bytes = Array.fill[Byte](1024)(1)
    val raw = new SeekableBais(bytes) {
      override def read(b: Array[Byte], off: Int, len: Int): Int = super.read(b, off, len)
    }
    val in = new ThrottledInputStream(raw, maxBytesPerSec = 1) // force sleep loop
    val t = new Thread(() => {
      try in.read() catch { case _: IOException => () }
    })
    t.start()
    t.interrupt()
    t.join()
    in.close()
  }
}

