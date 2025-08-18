import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.apache.hadoop.fs._


class MultiThreadedListing(threadPoolSize: Int) extends AutoCloseable {

  private val  executorService = Executors.newFixedThreadPool(threadPoolSize)
  private implicit val executor: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  def listFilesRecursively(fs: FileSystem, path: Path): List[FileStatus] = {
    def listFilesRecursivelyAsync(status: FileStatus): Future[List[FileStatus]] = Future {
      // we convert to a list because otherwise Future.sequence(futures) would ask for a implicit CanBuildFrom for array
      // which scala noob like me cannot solve
      val statuses: List[FileStatus] = fs.listStatus(status.getPath).toList
      val futures: List[Future[List[FileStatus]]] = statuses.map { fileStatus =>
        if (fileStatus.isDirectory) {
          listFilesRecursivelyAsync(fileStatus)
        } else {
          Future.successful(List(fileStatus))
        }
      }
      val futureSeq = Future.sequence(futures)
      val result = Await.result(futureSeq, Duration.Inf).flatten
      result
    }

    val result = Await.result(listFilesRecursivelyAsync(fs.getFileStatus(path)), Duration.Inf)
    result
  }

  override def close(): Unit = {
    executorService.shutdown()
  }
}
