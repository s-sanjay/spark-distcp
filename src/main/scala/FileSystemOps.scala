import org.apache.hadoop.conf.Configuration

import java.io.IOException
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

import java.net.URI
import scala.util.control.NonFatal

trait FileSystemOps {
  def getFileStatusOrThrow(fs: FileSystem, path: Path): FileStatus
  def getFileStatus(fs: FileSystem, path: Path): Option[FileStatus]
  def getLengthOrThrow(fs: FileSystem, path: Path, label: String): Long
  def requireSameLengthOrThrow(
                                sourceFS: FileSystem,
                                targetFS: FileSystem,
                                source: Path,
                                target: Path,
                                mismatchPrefix: String
                              ): Unit
  def createOrThrow(fs: FileSystem, path: Path, overwrite: Boolean = true): FSDataOutputStream
  def openOrThrow(fs: FileSystem, path: Path, bufferSize: Int = 4096): FSDataInputStream
  def getFileSystemOrThrow(uri: URI, conf: Configuration): FileSystem
  def getFileSystemOrThrow(path: Path, conf: Configuration): FileSystem
  def mkdirsOrThrow(fs: FileSystem, path: Path, perm: FsPermission): Unit
  def setPermissionOrThrow(fs: FileSystem, path: Path, perm: FsPermission): Unit
  def concatOrThrow(fs: FileSystem, target: Path, srcs: Array[Path]): Unit
  def deleteIfExistsOrThrow(fs: FileSystem, path: Path, recursive: Boolean): Unit
  def renameOrThrow(fs: FileSystem, src: Path, dst: Path): Unit
  def listStatusOrThrow(fs: FileSystem, path: Path): Seq[FileStatus]
}

// Default production implementation
object FileSystemUtil extends FileSystemOps {

  override def getFileStatusOrThrow(fs: FileSystem, path: Path): FileStatus = {
    try {
      val st = fs.getFileStatus(path)
      if (st == null) throw new IOException(s"File not found: ($path)")
      st
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error fetching file status for ($path)", e)
    }
  }

  override def getFileStatus(fs: FileSystem, path: Path): Option[FileStatus] = {
    try {
      Option(fs.getFileStatus(path))
    } catch {
      case _: IOException => None
      case NonFatal(_)    => None
    }
  }

  override def getLengthOrThrow(fs: FileSystem, path: Path, label: String): Long =
    getFileStatusOrThrow(fs, path).getLen

  override def requireSameLengthOrThrow(
                                         sourceFS: FileSystem,
                                         targetFS: FileSystem,
                                         source: Path,
                                         target: Path,
                                         mismatchPrefix: String
                                       ): Unit = {
    try {
      val srcLen = getLengthOrThrow(sourceFS, source, "source")
      val dstLen = getLengthOrThrow(targetFS, target, "target")
      if (srcLen != dstLen)
        throw new IOException(s"$mismatchPrefix$source ($srcLen) and target:$target ($dstLen)")
    } catch {
      case ioe: IOException =>
        throw new IOException(
          s"$mismatchPrefix$source and target:$target (metadata error: ${ioe.getMessage})",
          ioe
        )
    }
  }

  override def createOrThrow(fs: FileSystem, path: Path, overwrite: Boolean = true): FSDataOutputStream = {
    try {
      val out = fs.create(path, overwrite)
      if (out == null) throw new IOException(s"Failed to create file at $path (returned null)")
      out
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error creating file at $path", e)
    }
  }

  override def openOrThrow(fs: FileSystem, path: Path, bufferSize: Int = 4096): FSDataInputStream = {
    try {
      val in = fs.open(path, bufferSize)
      if (in == null) throw new IOException(s"Failed to open file at $path (returned null)")
      in
    } catch {
      case ioe: IOException => throw ioe                         // preserve original Hadoop IOEs
      case NonFatal(e)      => throw new IOException(s"Error opening file at $path", e)
    }
  }

  override def getFileSystemOrThrow(uri: URI, conf: Configuration): FileSystem = {
    try {
      val fs = FileSystem.get(uri, conf)
      if (fs == null) throw new IOException(s"Failed to get FileSystem for URI: $uri")
      fs
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error getting FileSystem for URI: $uri", e)
    }
  }

  override def getFileSystemOrThrow(path: Path, conf: Configuration): FileSystem = {
    try {
      val fs = path.getFileSystem(conf)
      if (fs == null) throw new IOException(s"Failed to get FileSystem for Path: $path")
      fs
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error getting FileSystem for Path: $path", e)
    }
  }

  override def mkdirsOrThrow(fs: FileSystem, path: Path, perm: FsPermission): Unit = {
    try {
      val ok = fs.mkdirs(path, perm)
      if (!ok) throw new IOException(s"Failed to mkdirs at $path with perm $perm")
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error creating directory at $path", e)
    }
  }

  override def setPermissionOrThrow(fs: FileSystem, path: Path, perm: FsPermission): Unit = {
    try {
      fs.setPermission(path, perm)
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error setting permission at $path", e)
    }
  }

  override def concatOrThrow(fs: FileSystem, target: Path, srcs: Array[Path]): Unit = {
    try {
      fs.concat(target, srcs)
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error concatenating into $target", e)
    }
  }

  override def deleteIfExistsOrThrow(fs: FileSystem, path: Path, recursive: Boolean): Unit = {
    try {
      if (getFileStatus(fs, path).isDefined) {
        val ok = fs.delete(path, recursive)
        if (!ok) throw new IOException(s"Failed to delete $path")
      }
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error deleting $path", e)
    }
  }

  override def renameOrThrow(fs: FileSystem, src: Path, dst: Path): Unit = {
    try {
      val ok = fs.rename(src, dst)
      if (!ok) throw new IOException(s"Failed to rename $src to $dst")
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error renaming $src to $dst", e)
    }
  }

  override def listStatusOrThrow(fs: FileSystem, path: Path): Seq[FileStatus] = {
    try {
      fs.listStatus(path).toSeq
    } catch {
      case ioe: IOException => throw ioe
      case NonFatal(e)      => throw new IOException(s"Error listing $path", e)
    }
  }
}
