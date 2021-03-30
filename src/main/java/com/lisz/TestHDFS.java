package com.lisz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {
	public Configuration conf = null;
	public FileSystem fs = null;

	@Before
	public void conn() throws Exception {
		conf = new Configuration(true);// true是自动加载xml信息
		// 指定URI和以那个用户对HDFS做操作
		fs = FileSystem.get(URI.create("hdfs://mycluster"), conf, "god"); // 具体生成哪个子类，参考了：<name>fs.defaultFS</name>
	}

	@Test
	public void mkdir() throws Exception{
		Path dir = new Path("/lisz");
		if (fs.exists(dir)) {
			fs.delete(dir, true);
		}
		fs.mkdirs(dir);
	}

	@Test
	public void upload() throws Exception{
		BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
		Path outfile = new Path("/lisz/out.txt");
		FSDataOutputStream output = fs.create(outfile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	@Test
	public void download() throws Exception{
		BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new File("./data/downloadedhello.txt")));
		Path inputfile = new Path("/lisz/out.txt");
		FSDataInputStream input = fs.open(inputfile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	// for i in `seq 100000`;do echo "hello lisz $i" >> ~/data.txt;done
	// hdfs dfs -D dfs.blocksize=1048576 -put ./data.txt
	@Test
	public void blocks() throws  Exception {
		Path file = new Path("/user/god/data.txt");
		FileStatus fileStatus = fs.getFileStatus(file);
		System.out.println(String.format("%s %s %s %s %s %s", fileStatus.getPermission(), fileStatus.getOwner(), fileStatus.getGroup(), fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getPath()));
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for (BlockLocation blockLocation : fileBlockLocations) { // 每次拿出一个块
			System.out.println(blockLocation); // 打印每个块的offset、size和坐落在哪些datanode上，方便了计算向数据移动
		}
		/*
		输出：
		rw-r--r-- god supergroup 1048576 1616884977327 hdfs://mycluster/user/god/data.txt
		0,1048576,hadoop-04,hadoop-02
		1048576,640319,hadoop-04,hadoop-02
		通过 fs.getFileStatus 和 fs.getFileBlockLocations 能拿到块的偏移量、大小和分布情况，从而很容易地计算向数据移动
		一个程序移动到第一个节点所在的块，一个程序移动到第二个节点所在的块。
		用户和程序读取的是文件这个级别，并不知道有块这个概念
		 */
		FSDataInputStream in = fs.open(file);// 面向文件打开的输入流，无论怎么读都是从文件的开始位置开始读
		/*
		blk_1073741831 (Block01)最后是hell，blk_1073741832（Block02）最开始是：o lisz 62335，可见是紧接着Block01的.文件存在存在：
		/var/bigdata/hadoop/ha/dfs/data/current/BP-720807851-192.168.1.6-1616800569583/current/finalized/subdir0/subdir0/
		 */

		//这里指定从哪里开始读取，有的程序并不是计算第一个快，所以它通过fs.getFileStatus 和 fs.getFileBlockLocations 拿到需要计算的那个块
		// 这叫数据本地化读取，计算向数据移动之后期望的是分治计算，只读取自己关心的（通过seek实现），同时具备距离的概念，优先和本地的DN获取数据
		// -- 框架的默认机制来计算距离
		in.seek(1048576); // 数据本地化读取：只有一个文件系统支持这个seek的时候才能很好的支持计算向数据移动，文件的读取可以用多个机器来完成并且各读各的
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
		System.out.println((char)in.read());
	}

	@After
	public void close() throws IOException {
		fs.close();
	}
}
