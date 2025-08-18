import org.apache.spark.SparkConf

/**
 * This enumeration, SparkDistPConf, defines a set of configuration options for the Spark-based DistCp implementation.
 * Each configuration option has a name and a default value, with types including String, Long, and Boolean.
 * The configuration values can be fetched from a SparkConf instance using the get method.
 * */
case object SparkDistPConf extends Enumeration {
  type SparkDistPConf = Value

  trait ValTrait[T] extends Value {
    def name(): String
    def value(): T
  }

  class StringVal(name: String, value: String) extends ValTrait[String] {
    override def value(): String = value
    override def name(): String = name
    override def id: Int = name.hashCode
  }

  class LongVal(name: String, value: Long) extends ValTrait[Long] {
    override def value(): Long = value
    override def name(): String = name
    override def id: Int = name.hashCode
  }

  class BooleanVal(name: String, value: Boolean) extends ValTrait[Boolean] {
    override def value(): Boolean = value
    override def name(): String = name
    override def id: Int = name.hashCode
  }

  def get[T](conf: SparkConf)(implicit ev: ValTrait[T]): T = {
    ev match {
      case _: StringVal => conf.get(ev.name(), ev.value().asInstanceOf[String]).asInstanceOf[T]
      case _: LongVal => conf.getLong(ev.name(), ev.value().asInstanceOf[Long]).asInstanceOf[T]
      case _: BooleanVal => conf.getBoolean(ev.name(), ev.value().asInstanceOf[Boolean]).asInstanceOf[T]
    }
  }

  // The size of the chunks to split large files into for copying,
  // with a default value of 4,000,000,000 bytes (4 GB).
  val CONF_DISTP_COPY_CHUNK_SIZE = new LongVal("distp.conf.copy.chunk.size", 4000000000L)
  // A flag to indicate if the listing should list all sub folders first and then copy or just list the given
  // input folder and lazily list the sub folder in different executors and copy them
  val CONF_DISTP_FLATTEN_LISTING_MODE = new BooleanVal("distp.conf.listing.flatten", false)
  // A flag to indicate if CRC checks should be skipped after the copy process, with a default value of false.
  // will also be used to skip file copy before starting the copy in the future
  val CONF_DISTP_COPY_SKIP_CRC_CHECK = new BooleanVal("distp.conf.copy.skipcrc", false)
  // The bandwidth to be used per copy operation, with a default value of 100,000,000 bytes (100 Mbps).
  val CONF_DISTP_COPY_BANDWIDTH_PER_COPY = new LongVal("distp.conf.copy.bandwdith.percopy", 100000000L)
  // The number of threads to be used for listing files in the flatten mode, with a default value of 10.
  val CONF_DISTP_FLATTEN_LISTING_THREAD_SIZE = new LongVal("distp.conf.copy.listing.thread", 10L)
}
