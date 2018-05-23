package netease.bigdata.mergetool

import netease.bigdata
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new IllegalArgumentException("args.length != 1")
    }
    new FileMerger().run(args)
  }
}
