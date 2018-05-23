package netease.bigdata.mergetool

import java.util
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}
import org.apache.hadoop.security.UserGroupInformation

import netease.bigdata.mergetool.domain.{Partition => DBRecord}
import netease.bigdata.mergetool.service.PartitionService
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

class FileMerger {

  val TOTAL_LEN_THRESHOLD = 1024L * 1024 * 1024 // 1gb
  val BLOCK_SIZE = 256L * 1024 * 1024
  val LOG = LoggerFactory.getLogger(this.getClass)
  /**
    * ThreadPool submitting job to SparkContext and waiting job to be finished.
    * use LimitedQueue to make 'submit' method blocking
    */
  val pool = new ThreadPoolExecutor(10, 50, 0L, TimeUnit.MILLISECONDS, new LimitedQueue[Runnable](200))
  val userName = UserGroupInformation.getCurrentUser().getUserName();
  val workPath = "/tmp/" + userName + "/parquet_mergetool"  // 集群tmp用户目录
  LOG.info("Working temp path : " + workPath)

  var fileSystem = FileSystem.get(new Configuration)
  var skipDeleteSource = false

  var sparkConf: SparkConf = null
  var sc : SparkContext = null
  var sqlContext : SQLContext = null

  val derbyDB = new PartitionService

  def run(args: Array[String]): Unit = {
    val dbPaths = getDbPathFromArgs(args)
    if(dbPaths.length == 0) {
      throw new IllegalArgumentException
    }
    
    skipDeleteSource = (args.length > 1 && args(1).equalsIgnoreCase("--skip-delete-source"))
    LOG.info("Skip delete source file : " + skipDeleteSource)
    var pathNameService = dbPaths(0).toUri().getScheme() + "://" + 
        dbPaths(0).toUri().getAuthority() + "/"
    LOG.info("Path " + dbPaths(0).toString() + " name service is " + pathNameService)

    val hadoopConf = new Configuration
    /**
     * 跨级群写数据需要制定nameservice
     */
    var currentFs = hadoopConf.get("fs.defaultFS");
    hadoopConf.set("fs.defaultFS", pathNameService)
    fileSystem = FileSystem.get(hadoopConf)

    sparkConf = new SparkConf().setAppName("parquet mergetool")
    sparkConf.set("spark.yarn.access.namenodes", currentFs + "," + pathNameService)
    sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("parquet.compression","UNCOMPRESSED")
    sqlContext = new SQLContext(sc)
    
    fileSystem.mkdirs(new Path(workPath))
    
    var allTaskCtxs:Stream[TaskContext] = null
    for(dbPath <- dbPaths){
        if(null==allTaskCtxs){
           allTaskCtxs = getPartitions(dbPath)
        }  else {
           allTaskCtxs = allTaskCtxs.append(getPartitions(dbPath))
        }
    }
    
    val partitions = allTaskCtxs.filter(context => {
      val f = context.inputPath
      if (!context.tmpPath.toString.contains(workPath)) {
        // It's a programmatic error, shouldn't happen.
        LOG.error("tmp path {} is invalid! filtered..", f)
        false
      } else {
        // clean up and prepare
        if (fileSystem.exists(context.tmpPath)) {
          LOG.warn("tmp path {} exists! removing it and then we can restart the task", context.tmpPathStr)
          fileSystem.delete(context.tmpPath, true)
        }
        true
      }
    })
    
    LOG.info("submit "+partitions.size+" tasks.")
    for (context <- partitions) {
      LOG.info("Task info ============== ")
      LOG.info("inputPath: "+context.inputStr)
      LOG.info("inputSize: "+context.inputFiles.size)
      LOG.info("tmpPath: "+context.tmpPathStr)
      pool.submit(context.getTask())
    }
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        pool.shutdownNow()
      }
    }))
    
    pool.shutdown()
    LOG.info("waiting job to be finished!")
    pool.awaitTermination(1L, TimeUnit.HOURS)
    fileSystem.close()
    sc.stop()
    LOG.info("successfully finished!")
    System.exit(0)
  }
  
  /**
   * 遍历每个父目录下的所有最小一层子目录
   */
  def getPartitions(curDir: Path): Stream[TaskContext] = {
    LOG.info("listing:" + curDir)
    val files = fileSystem.listStatus(curDir)
    var context: TaskContext = null

//    val numDir = files.filter(f => f.isDirectory).length
    val normalFiles = files.filter(f => f.isFile);
    if (normalFiles.length > 0) {
      val taskType = getTaskType(normalFiles)
      if (taskType != UnknownTaskType) {
        // 合并后的文件
        val targetFiles = normalFiles.filter(taskType.targetFilesFilter)
        // 待合并的文件 = 非合并后的文件 && 小于100M的文件
        val inputFiles = normalFiles.filter(taskType.inputFilesFilter)
        LOG.info("==========target files : " + targetFiles)
        LOG.info("==========input files : " + inputFiles)
        val dbRecord = derbyDB.getPartitionByDir(curDir.toString)

        if (dbRecord == null) {
          if(targetFiles.length > 0 && inputFiles.length==0){
            LOG.info("no record found, but {} has {} targetFiles. Merged already. Skipping..", curDir.toString, targetFiles.length)
          } else {
            // it's a new dir
            if (inputFiles.length > 1) {
              LOG.info("found parquet partition " + curDir + " with inputFiles:" + inputFiles.length)
              context = createTaskContext(taskType, fileSystem, curDir, inputFiles, getTmpPath(curDir))
            }
          }
        } else {
          // there's moving record, need some recovery
          if (targetFiles.length == dbRecord.getNumMerged) {
            //LOG.info("{} has complete {} merged files, cleaning up inputs...{}", curDir.toString, targetFiles.length, "")
            DBRecord.decodeInputPaths(dbRecord.getInputPaths).foreach(f => {
              LOG.info("deleting {}", f)
              fileSystem.delete(new Path(f), false)
            })
          } else if (targetFiles.length > dbRecord.getNumMerged){
            // it's not supposed to happen
            //LOG.error("{} has more merged files {} than expected {}", targetFiles, targetFiles.length, dbRecord.getNumMerged)
            throw new RuntimeException
          } else {
            // targetFiles are incomplete, cleanup and restart
            //  LOG.info("{} has incomplete {} merged files, cleaning up outputs and restart...{}", curDir.toString, targetFiles.length, "")
            targetFiles.foreach(f => {
              LOG.info("deleting {}", f)
              fileSystem.delete(f.getPath, false)
            })
          }
          derbyDB.deletePartition(curDir.toString)
        }
      }
    }
    val subDirs = scala.util.Random.shuffle(files.filter(f => f.isDirectory).toSeq)
    val tailStream = subDirs.toStream.flatMap(f => getPartitions(f.getPath))
    context match {
      case null => tailStream
      case context: TaskContext => context #:: tailStream
    }
  }

  private def createTaskContext(taskType: TaskType, fileSystem: FileSystem,
                                curDir: Path,
                                inputFiles: Array[FileStatus],
                                tmpPath: Path) : TaskContext = {
    taskType match {
      case ParquetTaskType =>
        new ParquetTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case LzoTaskType =>
        new LzoTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case GzipTaskType =>
        new GzipTaskContext(this, fileSystem, curDir, inputFiles, getTmpPath(curDir))
      case _ => null
    }
  }
  
  /**
   * 根据input path计算对应的tmp path
   */
  private def getTmpPath(input: Path): Path = {
    // 去除schema的path
    val relativePathStr = Path.getPathWithoutSchemeAndAuthority(input).toString
    val schema = input.toUri().getScheme
    val authority = input.toUri().getAuthority
   
    var tmpPath:Path = null
    if (relativePathStr.startsWith("/")) {
      tmpPath = new Path(workPath, "." + relativePathStr)
    } else {
      tmpPath = new Path(workPath, relativePathStr)
    }
    new Path(schema, authority, tmpPath.toString())
  }
  
  /**
   * 支持多目录配置, 以‘,’分割
   */
  private def getDbPathFromArgs(args: Array[String]): ArrayBuffer[Path] = {
    if (args == null || args.length < 1) {
      throw new IllegalArgumentException
    }
    
    val paths = ArrayBuffer[Path]()
    val dirs = args(0).split(",")
    for(dir <- dirs){
        LOG.info("Input dir: "+dir)
        //paths += Path.getPathWithoutSchemeAndAuthority(new Path(dir))
        paths += new Path(dir)
    }
    paths
  }
  
  private def getTaskType(files: Array[FileStatus]): TaskType = {
    val lzoCount = files.filter(f => f.getPath.getName.endsWith(".lzo")).length
    val gzipCount = files.filter(f => f.getPath.getName.endsWith(".gz")).length
    val parquetCount = files.filter(f => f.getPath.getName.endsWith(".parquet")).length
    val hasMultipleType = List(lzoCount, gzipCount, parquetCount).count(p => p > 0) > 1
    if (hasMultipleType) {
      UnknownTaskType
    } else if (lzoCount > 0) {
      LzoTaskType
    } else if (gzipCount > 0) {
      GzipTaskType
    } else if (parquetCount > 0) {
      ParquetTaskType
    } else if (files.length > 1 && files(0).getLen > 4) {
      // find one file with magic number "PAR1"
      // https://parquet.apache.org/documentation/latest/
      val bytes = new Array[Byte](4)
      val inputStream = fileSystem.open(files(0).getPath)
      inputStream.read(bytes)
      inputStream.close()
      if (util.Arrays.equals(ParquetTaskType.PARQUET_MAGIC_NUMBER, bytes)) {
        ParquetTaskType
      } else {
        UnknownTaskType
      }
    } else {
      UnknownTaskType
    }
  }
}
