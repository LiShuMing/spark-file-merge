package netease.bigdata.mergetool;

import netease.bigdata.mergetool.domain.Partition;
import netease.bigdata.mergetool.service.PartitionService;

import java.io.File;

public class MainTest {
  public static void main(String[] args) {
    System.out.println(new File(".").getAbsolutePath());
    PartitionService partitionService  = new PartitionService();
//    Partition p = new Partition("/tmp", 1, "/tmp1:||:tmp2");
//    partitionService.insertPartition(p);
//    Partition po = partitionService.getPartitionByDir("/tmp");
//    System.out.println(po.getDir());
    partitionService.deletePartition("/tmp");
  }
}
