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

//��ȡ���ŵĵĵ�ַ
	Configuration conf = new Configuration();
	String uri = "hdfs://10.28.150.93:9000/";
	FileSystem fs =FileSystem.get(URI.create(uri),conf);
	DistributedFileSystem hdfs = (DistributedFileSystem) fs; 
	Path fpath = new Path("hdfs://10.28.150.93:9000/copy/a.iso");
	FileStatus fileStatus = hdfs.getFileStatus(fpath);
	BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
	int blockLen = blkLocations.length;
	System.out.println("�ļ�����"+blockLen+"��");
	System.out.println("�ֱ��������»����ϣ�");
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

//��ȡ����datenode�ڵ�
public static void getHDFSNodes() throws Exception{
	Configuration conf = new Configuration();
	String uri = "hdfs://10.28.150.93:9000/";
	FileSystem fs =FileSystem.get(URI.create(uri),conf);
	DistributedFileSystem hdfs = (DistributedFileSystem) fs; 
	DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
	System.out.println("��Ⱥ����datenode�ڵ����£�");
	for( int i = 0 ; i < dataNodeStats.length ; ++i ){
		System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
	}
  }
}