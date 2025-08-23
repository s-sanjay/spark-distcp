import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class MultiThreadedListingSpec extends AnyFunSuite {

  test("lists files recursively with multiple threads") {
    val tmpDir = Files.createTempDirectory("mtl-")
    val dirPath = new Path(tmpDir.toUri)
    val conf = new Configuration(false)
    val fs = FileSystem.get(dirPath.toUri, conf).asInstanceOf[LocalFileSystem]

    // create a small tree: a/{b/{1,2}, c/{3}, 4}
    val a = new Path(dirPath, "a")
    val b = new Path(a, "b")
    val c = new Path(a, "c")
    val f1 = new Path(b, "1.txt")
    val f2 = new Path(b, "2.txt")
    val f3 = new Path(c, "3.txt")
    val f4 = new Path(a, "4.txt")
    fs.mkdirs(b)
    fs.mkdirs(c)
    fs.create(f1, true).close()
    fs.create(f2, true).close()
    fs.create(f3, true).close()
    fs.create(f4, true).close()

    val lister = new MultiThreadedListing(threadPoolSize = 4)
    try {
      val results: List[FileStatus] = lister.listFilesRecursively(fs, a)
      val names = results.map(_.getPath.getName).toSet
      assert(names == Set("1.txt", "2.txt", "3.txt", "4.txt"))
    } finally lister.close()
  }
}

