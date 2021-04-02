package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<TopNkey, IntWritable> {
	// 分区器的计算不能太复杂， 相同年月的返回值相同就可以了 （暂不考虑这么些造成的数据倾斜）
	@Override
	public int getPartition(TopNkey topNkey, IntWritable intWritable, int numPartitions) {
		return (topNkey.getYear() + topNkey.getMonth()) % numPartitions;
	}
}
