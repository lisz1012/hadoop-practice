package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyWordCount {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration(true);
		// 让框架知道是异构平台运行, 以不传Jar包的方式直接运行main方法的时候用这一句
		conf.set("mapreduce.app-submission.cross-platform", "true");

		Job job = Job.getInstance(conf);
		// 客户端会上传这个指定了的jar包，然后集群就会去下载放到classpath，然后就可以找到MyMapper和MyReducer类了。本地触发，发送到集群运行的时候需要这一句
		job.setJar("/Users/shuzheng/IdeaProjects/hadoop-practice/target/hadoop-practice-1.0-SNAPSHOT.jar");
        job.setJarByClass(MyWordCount.class);
        // Specify various job-specific parameters
		job.setJobName("lisz");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));


		TextInputFormat.addInputPath(job, new Path("/data/wc/input"));
		Path outputPath = new Path("/data/wc/output");
		if (outputPath.getFileSystem(conf).exists(outputPath)) outputPath.getFileSystem(conf).delete(outputPath, true);
		TextOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
	}
}
