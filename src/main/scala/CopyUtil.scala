import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class CopyUtil(fsOps: FileSystemOps = FileSystemUtil) extends Logging {

  private def copyChunk(srcFs: FileSystem, src: Path, dstFs: FileSystem, dst: Path, startPos: Long,
                        endPos: Long, chunkNumber: Long, copyRate: Long): Unit = {
    var dstOutputStream: FSDataOutputStream = null
    var srcInputStream: ThrottledInputStream = null
    try {
      srcInputStream = new ThrottledInputStream(fsOps.openOrThrow(srcFs, src), copyRate)
      val tempDstFile = new Path(dst.toString + s".chunk$chunkNumber")
      logInfo(s"copying $src to $tempDstFile at rate $copyRate")
      dstOutputStream = fsOps.createOrThrow(dstFs, tempDstFile)

      // TODO: make this configurable
      val buffer = new Array[Byte](8 * 1024)
      srcInputStream.seek(startPos)
      var bytesRead = srcInputStream.read(buffer)

      var totalRead: Long = startPos
      while (bytesRead > 0 && totalRead < endPos) {
        dstOutputStream.write(buffer, 0, bytesRead)
        totalRead += bytesRead
        bytesRead = srcInputStream.read(buffer)
      }
    } finally {
      if (dstOutputStream != null) dstOutputStream.close()
      if (srcInputStream != null) srcInputStream.close()
    }
  }

  private def copy(sparkContext: SparkContext, srcFs: FileSystem, srcFileStatus: FileStatus, dstFs: FileSystem, dst: Path): Unit = {
    val copyRate = SparkDistPConf.get[Long](sparkContext.getConf)(SparkDistPConf.CONF_DISTP_COPY_BANDWIDTH_PER_COPY)
    val chunkSize = SparkDistPConf.get[Long](sparkContext.getConf)(SparkDistPConf.CONF_DISTP_COPY_CHUNK_SIZE)
    assert(chunkSize > 0)
    val src = srcFileStatus.getPath
    val dstFileStatusOpt = fsOps.getFileStatus(dstFs, dst)
    val fileSize: Long = srcFileStatus.getLen
    val numChunks: Long = if (chunkSize == Long.MaxValue) 1L else (fileSize + chunkSize - 1) / chunkSize

    // if its a directory further parallelize
    if (srcFileStatus.isDirectory) {
      assert(!SparkDistPConf.get[Boolean](sparkContext.getConf)(SparkDistPConf.CONF_DISTP_FLATTEN_LISTING_MODE))
      logInfo(s"source $src is a directory so further parallelize it after mkdir the source")
      dstFileStatusOpt match {
        case None        => fsOps.mkdirsOrThrow(dstFs, dst, srcFileStatus.getPermission)
        case Some(_)     => fsOps.setPermissionOrThrow(dstFs, dst, srcFileStatus.getPermission)
      }
      doRun(srcFileStatus, dst, sparkContext, srcFs.getConf)
      return
    }

    val chunkNumbers: RDD[Long] = sparkContext.parallelize(0L until numChunks)
    chunkNumbers.foreach(chunkNumber => {
      val startPos = chunkNumber * chunkSize
      val endPos = math.min((chunkNumber + 1) * chunkSize, fileSize)
      copyChunk(srcFs, src, dstFs, dst, startPos, endPos, chunkNumber, copyRate)
    })


    // Concatenate all chunks into a single file the chunks will get auto deleted on concat
    val chunkFiles = (0 until numChunks).map { chunkNumber =>
      new Path(dst.toString + s".chunk$chunkNumber")
    }.toArray

    val tmpFile = chunkFiles.last
    if (chunkSize < Long.MaxValue) {
      logInfo("chunking is enabled")
      fsOps.concatOrThrow(dstFs, tmpFile, chunkFiles.dropRight(1))
    } else {
      logInfo("chunking not enabled")
      assert(chunkFiles.length == 1)
    }
    if (dstFileStatusOpt.isDefined) {
      fsOps.deleteIfExistsOrThrow(dstFs, dst, recursive = false)
    }
    fsOps.renameOrThrow(dstFs, tmpFile, dst)
    fsOps.setPermissionOrThrow(dstFs, dst, srcFileStatus.getPermission)

    CheckSumUtil.compareFileLengthsAndChecksums(srcFs, src, null, dstFs, dst,
      SparkDistPConf.get[Boolean](sparkContext.getConf)(SparkDistPConf.CONF_DISTP_COPY_SKIP_CRC_CHECK))
  }

  def doRun(srcFileStatus: FileStatus, dstPath: Path, sc: SparkContext, hadoopConf: Configuration): Unit = {
    try {
      val srcFs = fsOps.getFileSystemOrThrow(srcFileStatus.getPath, hadoopConf)
      val srcFiles: Seq[FileStatus] = listStatus(srcFs, srcFileStatus.getPath, sc.getConf)
      val srcFileTuples = srcFiles.par.map(srcFileStatus => {
        val relativePath = srcFileStatus.getPath.getName
        val dstTgtPath = new Path(dstPath, relativePath)
        logInfo(s"copying ${srcFileStatus.getPath} with name $relativePath to $dstTgtPath")
        (srcFileStatus, dstTgtPath)
      })

      val filePairsRDD = sc.parallelize(srcFileTuples.seq)

      filePairsRDD.foreach {
        case (src, dst) =>
          val srcFs = FileSystem.get(src.getPath.toUri, hadoopConf)
          val dstFs = FileSystem.get(dst.toUri, hadoopConf)
          copy(sc, srcFs, src, dstFs, dst)
      }
    }
  }

  private def listStatus(fs: FileSystem, path: Path, sparkConf: SparkConf): Seq[FileStatus] = {
    if (SparkDistPConf.get[Boolean](sparkConf)(SparkDistPConf.CONF_DISTP_FLATTEN_LISTING_MODE)) {
      new MultiThreadedListing(SparkDistPConf.get[Long](sparkConf)(SparkDistPConf.CONF_DISTP_FLATTEN_LISTING_THREAD_SIZE).toInt)
        .listFilesRecursively(fs, path)
    } else {
      // in this mode, we only list the parent level path and then let the executors list sub directories when needed
      // so there is no need for multi threading
      fsOps.listStatusOrThrow(fs, path)
    }
  }
}
