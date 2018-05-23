package netease.bigdata.mergetool.mappers;

import netease.bigdata.mergetool.domain.Partition;


public interface PartitionMapper {

  void insertPartition(Partition partition);

  Partition getPartitionByDir(String dir);

  void deletePartition(String dir);
}
