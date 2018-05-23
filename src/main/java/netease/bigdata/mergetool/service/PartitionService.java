package netease.bigdata.mergetool.service;

import netease.bigdata.mergetool.domain.Partition;
import netease.bigdata.mergetool.mappers.PartitionMapper;

import org.apache.ibatis.session.SqlSession;

public class PartitionService {

  public PartitionService() {
    // try open session
    getPartitionByDir("");
  }

  public void insertPartition(Partition partition) {
    SqlSession sqlSession = MyBatisUtil.getSqlSessionFactory().openSession();
    try {
      PartitionMapper partitionMapper = sqlSession.getMapper(PartitionMapper.class);
      partitionMapper.insertPartition(partition);
      sqlSession.commit();
    } finally {
      sqlSession.close();
    }
  }

  public Partition getPartitionByDir(String dir) {
    SqlSession sqlSession = MyBatisUtil.getSqlSessionFactory().openSession();
    try {
      PartitionMapper partitionMapper = sqlSession.getMapper(PartitionMapper.class);
      return partitionMapper.getPartitionByDir(dir);
    } finally {
      sqlSession.close();
    }
  }

  public void deletePartition(String dir) {
    SqlSession sqlSession = MyBatisUtil.getSqlSessionFactory().openSession();
    try {
      PartitionMapper partitionMapper = sqlSession.getMapper(PartitionMapper.class);
      partitionMapper.deletePartition(dir);
      sqlSession.commit();
    } finally {
      sqlSession.close();
    }
  }
}