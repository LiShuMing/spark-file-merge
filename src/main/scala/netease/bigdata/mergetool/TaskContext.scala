package netease.bigdata.mergetool

import java.util.Collections

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.AclEntry
import org.apache.hadoop.fs.permission.AclUtil
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.hadoop.compression.lzo.LzopCodec
import com.hadoop.mapreduce.LzoTextInputFormat

import netease.bigdata.mergetool.domain.{ Partition => DBRecord }

abstract class TaskContext(val fileMerger: FileMerger, val fileSystem: FileSystem,
                           val inputPath: Path,
                           val inputFiles: Array[FileStatus],
                           val tmpPath: Path,
                           val taskType: TaskType) {
  val LOG = fileMerger.LOG
  val derbyDB = fileMerger.derbyDB
  val inputStr = inputPath.toString
  val tmpPathStr = tmpPath.toString
  lazy val aclEntries = {
    val entries = fileSystem.getAclStatus(inputFiles(0).getPath).getEntries
    AclUtil.getAclFromPermAndEntries(getPermission,
      if (entries != null) entries else Collections.emptyList[AclEntry])
  }

  lazy val inputSize = {
    inputFiles.foldLeft(0L)((b, f) => b + f.getLen)
  }

  // calculation: to avoid files like  257M ~ 266M which causes many small blocks,
  // We add a new file with a new big block
  // not used if it's parquet type
  lazy val numPartitions = {
      if(inputSize < fileMerger.BLOCK_SIZE) 1
      else (inputSize / (fileMerger.BLOCK_SIZE - 10L * 1024 * 1024)).toInt
  }

  def getOwner: String = {
    inputFiles(0).getOwner
  }

  def getGroup: String = {
    inputFiles(0).getGroup
  }

  def getPermission: FsPermission = {
    inputFiles(0).getPermission
  }

  def readWriteFile() : Unit

  def getTask(): Runnable =  new Runnable {
    override def run(): Unit = {
      try {
        if (!tmpPathStr.contains(fileMerger.workPath)) {
          LOG.warn("tmp path {} is invalid!", tmpPathStr)
          return
        }
        LOG.info("start writing, input:" + inputStr +
          ", tmp path:" + tmpPathStr)

        readWriteFile()

        LOG.info("finished writing. set properties to file..")

        val tmpNewFiles = fileSystem.listStatus(tmpPath)
          .filter(f => f.getPath.toString.endsWith(taskType.getTargetFileExtension))
        
        //TODO 没明白为什么这里判定如果合并后只有一个合法文件，就直接返回
        /*if (tmpNewFiles.length == 1) {
          LOG.error("generated {} file {} doesn't exist! {}", taskType.getTargetFileExtension, tmpPathStr, "")
          return
        }*/
        
        //TODO 普通用户执行这一步时会报错，先注释掉试试看
        //setPermissionOfNewFiles(tmpNewFiles)

        if(fileMerger.skipDeleteSource) {
            LOG.info("Skip delete source file and replace by target file. job finish success.")
            LOG.info("Source : " + inputPath)
            LOG.info("Target : " + tmpPath)
        }

        LOG.info("start transaction of moving files back")
        val dbRecord = new DBRecord(inputPath.toString, tmpNewFiles.length,
          DBRecord.encodeInputPaths(inputFiles.map(f => f.getPath.toString)))
        derbyDB.insertPartition(dbRecord)

        val targetPaths = taskType.newTargetPaths(inputPath, tmpNewFiles.length)

        for(i <- 0.until(tmpNewFiles.length)) {
          fileSystem.rename(tmpNewFiles(i).getPath, targetPaths(i))
          LOG.info("renamed file " + tmpNewFiles(i).getPath + " to " + targetPaths(i))
        }

        // clean up
        LOG.info("cleaning up...")
        LOG.info("deleting {}", tmpPath)
        fileSystem.delete(tmpPath, true)
        inputFiles.foreach(f => {
          LOG.info("deleting {}", f.getPath)
          fileSystem.delete(f.getPath, false)
        })
        derbyDB.deletePartition(inputPath.toString)
        LOG.info("job succeeded!")
      } catch {
        case e: Throwable => LOG.error("job failed.", e)
      }
    }
  }

  def setPermissionOfNewFiles(newFiles: Array[FileStatus]): Unit = {
    for (newFile <- newFiles) {
      fileSystem.setOwner(newFile.getPath, getOwner, getGroup)
      if (aclEntries.size() > 3) {
        // minimal acl is always required, which is owner:group:other,
        // but only the 4th Entry is recorded in AclFeature,
        fileSystem.setAcl(newFile.getPath, aclEntries)
      } else {
        // the minimal three ones is recorded in permission bit.
        fileSystem.setPermission(newFile.getPath, getPermission)
      }
    }
  }
}

class ParquetTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                         inputPath: Path,
                         inputFiles: Array[FileStatus],
                         tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, ParquetTaskType) {
  
  def newParquetRDD(src: String) = {
     fileMerger.sqlContext.read.parquet(src)
  }

  override def readWriteFile() = {
    var totalFileLength:Long = 0
    val inputFilePaths = inputFiles.map { f => {
       totalFileLength += f.getLen   
       f.getPath.toString()
    } }
    
    val totalFileDF = fileMerger.sqlContext.read.parquet(inputFilePaths : _*)
    
    // 每512M（压缩前）一个分区，输出一个文件
    val result = (totalFileLength / (512L * 1024 * 1024)).toInt
    
    val partitionSize = if (result <= 1) 1 else (result+1)
    
    LOG.info("All input files byte size is {}, and write into {} target files!", totalFileLength, partitionSize)
    
    totalFileDF.coalesce(partitionSize).write.parquet(tmpPathStr)
  }
}

class LzoTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                     inputPath: Path,
                     inputFiles: Array[FileStatus],
                     tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, LzoTaskType) {

  def newLzoRdd(src: String) = {
    fileMerger.sc.newAPIHadoopFile(src,
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])
  }

  def saveLzoFile(rdd: RDD[(LongWritable, Text)], targetPathStr: String) = {
    LOG.info("Number Partitions for Lzo format : {}.", numPartitions);
    rdd.coalesce(numPartitions).map(record => (NullWritable.get(), record._2))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](targetPathStr,
      classOf[com.hadoop.compression.lzo.LzopCodec])
  }

  override def readWriteFile() = {
    val rdds = inputFiles.map(f => newLzoRdd(f.getPath.toString))
    val unionRdd = rdds.reduceLeft((b, a) => b.union(a))
    saveLzoFile(unionRdd, tmpPathStr)
  }
}


class GzipTaskContext(fileMerger: FileMerger, fileSystem: FileSystem,
                      inputPath: Path,
                      inputFiles: Array[FileStatus],
                      tmpPath: Path)
  extends TaskContext(fileMerger, fileSystem, inputPath, inputFiles, tmpPath, GzipTaskType) {

  override def readWriteFile() = {
    val rdds = inputFiles.map(f => fileMerger.sc.textFile(f.getPath.toString))
    val unionRdd = rdds.reduceLeft((b, a) => b.union(a))
    LOG.info("Number Partitions for Gzip format : {}.", numPartitions);
    unionRdd.coalesce(numPartitions).saveAsTextFile(tmpPathStr, classOf[GzipCodec])
  }
}
