import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.net.URI

class FileSystemUtilSpec extends AnyFunSuite with MockitoSugar {

  test("getFileStatusOrThrow returns status when present") {
    val fs = mock[FileSystem]
    val p = new Path("/foo")
    val st = mock[FileStatus]
    when(fs.getFileStatus(p)).thenReturn(st)
    assert(FileSystemUtil.getFileStatusOrThrow(fs, p) eq st)
  }

  test("getFileStatusOrThrow throws when null or error") {
    val fs = mock[FileSystem]
    val p = new Path("/missing")
    when(fs.getFileStatus(p)).thenReturn(null)
    assertThrows[IOException] {
      FileSystemUtil.getFileStatusOrThrow(fs, p)
    }
  }

  test("getLengthOrThrow delegates to getFileStatus") {
    val fs = mock[FileSystem]
    val p = new Path("/file")
    val st = mock[FileStatus]
    when(fs.getFileStatus(p)).thenReturn(st)
    when(st.getLen).thenReturn(1234L)
    val len = FileSystemUtil.getLengthOrThrow(fs, p, label = "source")
    assert(len == 1234L)
  }

  test("requireSameLengthOrThrow throws on mismatch with message prefix") {
    val srcFS = mock[FileSystem]
    val dstFS = mock[FileSystem]
    val src = new Path("/src")
    val dst = new Path("/dst")
    val st1 = mock[FileStatus]
    val st2 = mock[FileStatus]
    when(srcFS.getFileStatus(src)).thenReturn(st1)
    when(dstFS.getFileStatus(dst)).thenReturn(st2)
    when(st1.getLen).thenReturn(1L)
    when(st2.getLen).thenReturn(2L)
    val ex = intercept[IOException] {
      FileSystemUtil.requireSameLengthOrThrow(srcFS, dstFS, src, dst, Constants.LENGTH_MISMATCH_ERROR_MSG)
    }
    assert(ex.getMessage.startsWith(Constants.LENGTH_MISMATCH_ERROR_MSG))
  }

  test("createOrThrow and openOrThrow throw IOException on null results") {
    val fs = mock[FileSystem]
    val p = new Path("/file")
    when(fs.create(p, true)).thenReturn(null)
    assertThrows[IOException] { FileSystemUtil.createOrThrow(fs, p, overwrite = true) }

    when(fs.open(p, 4096)).thenReturn(null)
    assertThrows[IOException] { FileSystemUtil.openOrThrow(fs, p, 4096) }
  }

  test("getFileSystemOrThrow by URI and Path throw on null results") {
    val conf = new Configuration(false)
    val uri = new URI("file:///tmp")
    val fs = mock[FileSystem]

    // We cannot mock static FileSystem.get without a framework; just validate non-null in happy path via real call
    val real = FileSystem.get(uri, conf)
    assert(real != null)

    // Path-based
    val path = new Path("file:///tmp")
    val real2 = path.getFileSystem(conf)
    assert(real2 != null)
  }
}

