package netease.bigdata.mergetool

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.{Path, FileStatus}

abstract class TaskType {
  val SMALL_FILE_LEN_THRESHOLD = 100L * 1024 * 1024 // 100MB
  def getTargetFileExtension() : String
  def targetFilesFilter(f: FileStatus) : Boolean
  def inputFilesFilter(f: FileStatus) = {
    !targetFilesFilter(f) && smallfileFilter(f)
  }
  def smallfileFilter(f: FileStatus) = {
    f.getLen < SMALL_FILE_LEN_THRESHOLD
  }
  def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path]
}
case object LzoTaskType extends TaskType{

  override def getTargetFileExtension(): String = ".lzo"

  override def targetFilesFilter(f : FileStatus)  = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(".lzo")
  }
  
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + ".lzo")).toArray
  }
}

case object GzipTaskType extends TaskType{

  override def getTargetFileExtension(): String = ".gz"

  override def targetFilesFilter(f : FileStatus)  = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(".gz")
  }
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + ".gz")).toArray
  }
}

case object ParquetTaskType extends TaskType{
  val PARQUET_MAGIC_NUMBER = "PAR1".getBytes(StandardCharsets.US_ASCII)
  
  // .gz.parquet => .parquet
  override def getTargetFileExtension(): String = ".parquet"

  override def targetFilesFilter(f : FileStatus)  = {
    val filename = f.getPath.getName
    filename.startsWith("merged__") && filename.endsWith(".parquet")
  }
  
  /**
   * 生成输出文件名
   * 这里为生成的文件名增加时间戳
   */
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = {
    // TODO 这里为什么要 getParent ??? 
    //0.until(numMerged).map(i => new Path(intputPath.getParent, "merged__" + i + ".parquet")).toArray
    0.until(numMerged).map(i => new Path(intputPath, "merged__" + i + "__" +System.currentTimeMillis() + ".parquet")).toArray
  }
}

case object UnknownTaskType extends TaskType {
  override def getTargetFileExtension(): String = null
  override def targetFilesFilter(f : FileStatus) = false
  override def newTargetPaths(intputPath :Path, numMerged :Int) :  Array[Path] = null
}
