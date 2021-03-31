package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration(true);
		// 去掉带着 -D 各个参数之后剩下的参数，组成一个数组。这里就能用 -D mapreduce.job.reduces=2 /data/wc/input /data/wc/outputargs 运行了
		GenericOptionsParser parser = new GenericOptionsParser(conf, args); // 工具类帮我们把通用的-D类的属性直接set到conf里，留下commandOptions
		String otherArgs[] = parser.getRemainingArgs(); // 这里就可以取出输入输出路径了

		// 让框架知道是异构平台运行, 以不传Jar包的方式直接运行main方法的时候用这一句
		conf.set("mapreduce.app-submission.cross-platform", "true");
//		String s = conf.get("mapreduce.framework.name");
//		System.out.println(s);
		conf.set("mapreduce.framework.name", "yarn");

		Job job = Job.getInstance(conf);
		// 客户端会上传这个指定了的jar包，然后集群就会去下载放到classpath，然后就可以找到MyMapper和MyReducer类了。本地触发，发送到集群运行的时候需要这一句
		job.setJar("/Users/shuzheng/IdeaProjects/hadoop-practice/target/hadoop-practice-1.0-SNAPSHOT.jar");
        job.setJarByClass(MyWordCount.class);
        // Specify various job-specific parameters
		job.setJobName("lisz");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));


		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path outputPath = new Path(otherArgs[1]);
		if (outputPath.getFileSystem(conf).exists(outputPath)) outputPath.getFileSystem(conf).delete(outputPath, true);
		TextOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // 程序提交的时候写 -D mapreduce.job.reduces=3 就可以灵活指定启动多少个Reducer了
        //job.setNumReduceTasks(2);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
	}
}
