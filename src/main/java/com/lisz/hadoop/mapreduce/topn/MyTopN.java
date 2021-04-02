package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyTopN {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration(true);
		String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);

		job.setJarByClass(MyTopN.class);
		job.setJobName("Top N");
		// 作为初学者，关注的是Client端的代码梳理，下面的代码要熟练, 如果把这块写明白了，也就真的知道这个作业的开发原理了
		// MR的作者屏蔽了很多技术底层的细节，我们只需要关心和业务相关的点就好
		// input
		TextInputFormat.addInputPath(job, new Path(other[0]));
		Path outfile = new Path(other[1]);
		if (outfile.getFileSystem(conf).exists(outfile)) outfile.getFileSystem(conf).delete(outfile, true);
		TextOutputFormat.setOutputPath(job, outfile);
		// key
		job.setMapperClass(TopMapper.class);
		job.setMapOutputKeyClass(TopNkey.class);
		job.setMapOutputValueClass(IntWritable.class);

		// map
		// partitioner 按年月分区，分区 > 分组。甚至还可以按年分区。分区器的潜台词：相同的key获得相同的分区号。分区器非常灵活
		job.setPartitionerClass(TopNPartitioner.class);
		// sortComparator 年月温度，且温度倒序
		job.setSortComparatorClass(TopNComparator.class);
		// combiner
		//job.setCombinerClass();

		// reduce
		// groupingComparator 相同的key到了同一个分区，但是同一个分区中可能有不同的key。相同的key可能不挨着
		job.setGroupingComparatorClass(TopNGroupingComparator.class);
		// reduce
		job.setReducerClass(TopNReducer.class);

		job.waitForCompletion(true);
	}
}
