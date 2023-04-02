import org.apache.spark.internal.Logging
import org.apache.hadoop.fs._

import java.io.IOException
import scala.collection.mutable

object CheckSumUtil extends Logging {

  sealed trait ChecksumComparison

  private object ChecksumComparison {
    case object TRUE extends ChecksumComparison
    case object FALSE extends ChecksumComparison
    case object INCOMPATIBLE extends ChecksumComparison
  }

  /**
   * Utility to compare checksums for the paths specified. This assumes the underlying filesystems implement checksum
   *
   * If checksums can't be retrieved, it doesn't fail the test
   * Only time the comparison would fail is when checksums are
   * available and they don't match
   *
   * @param sourceFS       FileSystem for the source path.
   * @param source         The source path.
   * @param sourceChecksum The checksum of the source file. If it is null we
   *                       still need to retrieve it through sourceFS.
   * @param targetFS       FileSystem for the target path.
   * @param target         The target path.
   * @return If either checksum couldn't be retrieved, the function returns
   *         ChecksumComparison.INCOMPATIBLE. If checksums are retrieved, the function returns true if they match,
   *         and false otherwise.
   * @throws IOException if there's an exception while retrieving checksums.
   */
  private def checksumsAreEqual(sourceFS: FileSystem, source: Path, sourceChecksum: FileChecksum,
                                targetFS: FileSystem, target: Path, sourceLen: Long): ChecksumComparison = {
    var targetChecksum: FileChecksum = null
    try {
      targetChecksum = Option(targetFS.getFileChecksum(target)).orNull
      val checksum = Option(sourceChecksum).getOrElse(sourceFS.getFileChecksum(source, sourceLen))
      if (checksum != null && targetChecksum != null) {
        if (checksum.equals(targetChecksum)) {
          return ChecksumComparison.TRUE
        } else {
          return ChecksumComparison.FALSE
        }
      }
    } catch {
      case e: IOException =>
        logError(s"Unable to retrieve checksum for $source or $target", e)
    }
    ChecksumComparison.INCOMPATIBLE
  }

  /**
   * Utility to compare file lengths and checksums for source and target.
   *
   * @param sourceFS       FileSystem for the source path.
   * @param source         The source path.
   * @param sourceChecksum The checksum of the source file. If it is null we
   *                       still need to retrieve it through sourceFS.
   * @param targetFS       FileSystem for the target path.
   * @param target         The target path.
   * @param skipCrc        The flag to indicate whether to skip checksums.
   * @throws IOException if there's a mismatch in file lengths or checksums.
   */
  def compareFileLengthsAndChecksums(sourceFS: FileSystem, source: Path,
                                     sourceChecksum: FileChecksum, targetFS: FileSystem,
                                     target: Path, skipCrc: Boolean): Unit = {
    val srcStatus = sourceFS.getFileStatus(source)
    val dstStatus = targetFS.getFileStatus(target)
    val srcLen = srcStatus.getLen
    val targetLen = dstStatus.getLen
    if (srcLen != targetLen) {
      throw new IOException(s"${Constants.LENGTH_MISMATCH_ERROR_MSG}$source ($srcLen) and target:$target ($targetLen)")
    }

    // At this point, src & dest lengths are same. if length==0, we skip checksum
    if (srcLen != 0 && !skipCrc) {
      val checksumComparison = checksumsAreEqual(sourceFS, source, sourceChecksum, targetFS, target, srcLen)
      val checksumResult = checksumComparison != ChecksumComparison.FALSE
      if (!checksumResult) {
        val errorMessage = new mutable.StringBuilder(Constants.CHECKSUM_MISMATCH_ERROR_MSG)
          .append(source).append(" and ").append(target).append(".")
        var addSkipHint = false
        val srcScheme = sourceFS.getScheme
        val targetScheme = targetFS.getScheme
        if (!srcScheme.equals(targetScheme)) {
          // the filesystems are different and they aren't both hdfs connectors
          errorMessage.append("Source and destination filesystems are of different types\n")
            .append("Their checksum algorithms may be incompatible")
          addSkipHint = true
        } else if (srcStatus.getBlockSize != dstStatus.getBlockSize) {
          errorMessage.append(" Source and target differ in block-size.\n")
          addSkipHint = true
        }
        if (addSkipHint) {
          errorMessage.append(" You can choose file-level checksum validation via " +
            "-Ddfs.checksum.combine.mode=COMPOSITE_CRC when block-sizes or filesystems are different.")
            .append(" Or you can skip checksum-checks altogether.\n")
            .append(" (NOTE: By skipping checksums, one runs the risk of masking data-corruption during file-transfer.)\n")
        }
        val err = errorMessage.toString
        logError(err)
        throw new IOException(err)
      }
    }
  }
}
