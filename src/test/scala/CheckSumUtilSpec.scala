import org.apache.hadoop.fs.{FileChecksum, FileStatus, FileSystem, Path}
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException

class CheckSumUtilSpec extends AnyFunSuite with MockitoSugar {

  test("throws when lengths differ") {
    val srcFS = mock[FileSystem]
    val dstFS = mock[FileSystem]
    val src = new Path("/src/fileA")
    val dst = new Path("/dst/fileA")

    val srcStatus = mock[FileStatus]
    val dstStatus = mock[FileStatus]

    when(srcFS.getFileStatus(src)).thenReturn(srcStatus)
    when(dstFS.getFileStatus(dst)).thenReturn(dstStatus)
    when(srcStatus.getLen).thenReturn(100L)
    when(dstStatus.getLen).thenReturn(200L)

    val ex = intercept[IOException] {
      CheckSumUtil.compareFileLengthsAndChecksums(srcFS, src, null, dstFS, dst, skipCrc = false)
    }
    assert(ex.getMessage.startsWith(Constants.LENGTH_MISMATCH_ERROR_MSG))
    assert(ex.getMessage.contains(src.toString))
  }

  test("skips checksum when skipCrc is true") {
    val srcFS = mock[FileSystem]
    val dstFS = mock[FileSystem]
    val src = new Path("/src/fileA")
    val dst = new Path("/dst/fileA")

    val srcStatus = mock[FileStatus]
    val dstStatus = mock[FileStatus]

    when(srcFS.getFileStatus(src)).thenReturn(srcStatus)
    when(dstFS.getFileStatus(dst)).thenReturn(dstStatus)
    when(srcStatus.getLen).thenReturn(1024L)
    when(dstStatus.getLen).thenReturn(1024L)

    // should not throw
    CheckSumUtil.compareFileLengthsAndChecksums(srcFS, src, null, dstFS, dst, skipCrc = true)
  }

  test("passes when checksums match and lengths equal") {
    val srcFS = mock[FileSystem]
    val dstFS = mock[FileSystem]
    val src = new Path("/src/fileA")
    val dst = new Path("/dst/fileA")

    val srcStatus = mock[FileStatus]
    val dstStatus = mock[FileStatus]
    val checksum = mock[FileChecksum]

    when(srcFS.getFileStatus(src)).thenReturn(srcStatus)
    when(dstFS.getFileStatus(dst)).thenReturn(dstStatus)
    when(srcStatus.getLen).thenReturn(4096L)
    when(dstStatus.getLen).thenReturn(4096L)
    when(srcStatus.getBlockSize).thenReturn(128L * 1024 * 1024)
    when(dstStatus.getBlockSize).thenReturn(128L * 1024 * 1024)
    when(srcFS.getScheme).thenReturn("file")
    when(dstFS.getScheme).thenReturn("file")

    when(srcFS.getFileChecksum(src, 4096L)).thenReturn(checksum)
    when(dstFS.getFileChecksum(dst)).thenReturn(checksum)

    // should not throw
    CheckSumUtil.compareFileLengthsAndChecksums(srcFS, src, null, dstFS, dst, skipCrc = false)
  }

  test("throws when checksums differ and lengths equal") {
    val srcFS = mock[FileSystem]
    val dstFS = mock[FileSystem]
    val src = new Path("/src/fileA")
    val dst = new Path("/dst/fileA")

    val srcStatus = mock[FileStatus]
    val dstStatus = mock[FileStatus]
    val srcChecksum = mock[FileChecksum]
    val dstChecksum = mock[FileChecksum]

    when(srcFS.getFileStatus(src)).thenReturn(srcStatus)
    when(dstFS.getFileStatus(dst)).thenReturn(dstStatus)
    when(srcStatus.getLen).thenReturn(4096L)
    when(dstStatus.getLen).thenReturn(4096L)
    when(srcStatus.getBlockSize).thenReturn(128L * 1024 * 1024)
    when(dstStatus.getBlockSize).thenReturn(128L * 1024 * 1024)
    when(srcFS.getScheme).thenReturn("file")
    when(dstFS.getScheme).thenReturn("file")

    when(srcFS.getFileChecksum(src, 4096L)).thenReturn(srcChecksum)
    when(dstFS.getFileChecksum(dst)).thenReturn(dstChecksum)

    // by default different mock instances are not equal
    val ex = intercept[IOException] {
      CheckSumUtil.compareFileLengthsAndChecksums(srcFS, src, null, dstFS, dst, skipCrc = false)
    }
    assert(ex.getMessage.startsWith(Constants.CHECKSUM_MISMATCH_ERROR_MSG))
    assert(ex.getMessage.contains(src.toString))
    assert(ex.getMessage.contains(dst.toString))
  }
}

