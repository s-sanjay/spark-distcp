import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import java.io.IOException


object SparkDistCp extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logInfo("Usage: SparkDistCp <src> <dst>")
      sys.exit(1)
    }

    val srcPath = new Path(args(0))
    val dstPath = new Path(args(1))

    val sparkConf = new SparkConf().setAppName("SparkDistCp")
    val sc = new SparkContext(sparkConf)

    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    execute(srcPath, dstPath, sc, hadoopConf, fs)

    logInfo(s"Copy operation completed. Files copied from $srcPath to $dstPath.")
    sc.stop()
  }

  private def execute(srcPath: Path, dstPath: Path, sc: SparkContext, hadoopConf: Configuration, fs: FileSystem): Unit = {
    val srcFileStatus = fs.getFileStatus(srcPath)
    if (srcFileStatus == null) {
      logError(s"Source path does not exist: $srcPath")
      throw new IOException(s"Source path does not exist: $srcPath")
    }
    if (fs.exists(dstPath)) {
      logInfo(s"Destination path already exists: $dstPath just set permission")
      fs.setPermission(dstPath, fs.getFileStatus(srcPath).getPermission)
    } else {
      logInfo(s"create top level dest: $dstPath and set permission")
      fs.mkdirs(dstPath, srcFileStatus.getPermission)
    }
    val util = new CopyUtil();
    util.doRun(srcFileStatus, dstPath, sc, hadoopConf)
  }
}

