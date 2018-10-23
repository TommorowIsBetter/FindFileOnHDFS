package com.cn.plase;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;


public class FindOnHDFS {
public static void main(String[] args) throws Exception{
	getHDFSNodes();
	getFileLocal();
}


public static void getFileLocal() throws Exception {

//获取块存放的的地址
	Configuration conf = new Configuration();
	String uri = "hdfs://10.28.150.93:9000/";
	FileSystem fs =FileSystem.get(URI.create(uri),conf);
	DistributedFileSystem hdfs = (DistributedFileSystem) fs; 
	Path fpath = new Path("hdfs://10.28.150.93:9000/copy/a.iso");
	FileStatus fileStatus = hdfs.getFileStatus(fpath);
	BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
	int blockLen = blkLocations.length;
	System.out.println("文件共有"+blockLen+"块");
	System.out.println("分别存放在以下机器上：");
	if(blockLen==1){
		  String[] hosts = blkLocations[0].getHosts();
		  System.out.println("block:"+hosts[0]);
	}else{
		for(int i = 0 ; i < blkLocations[i].getHosts().length ; ++i ){
		    String[] hosts = blkLocations[i].getHosts();
		    System.out.println("block:"+hosts[i]);
        }
	}
}

//获取所有datenode节点
public static void getHDFSNodes() throws Exception{
	Configuration conf = new Configuration();
	String uri = "hdfs://10.28.150.93:9000/";
	FileSystem fs =FileSystem.get(URI.create(uri),conf);
	DistributedFileSystem hdfs = (DistributedFileSystem) fs; 
	DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
	System.out.println("集群共有datenode节点如下：");
	for( int i = 0 ; i < dataNodeStats.length ; ++i ){
		System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
	}
  }
}