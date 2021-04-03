package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text names = new Text();
	// 0表示key中两个人是直接好友；1表示列表中两人是间接好友，后面reduce一旦发现两人是直接好友，就不计算了，累加结果就是公共好友数量
	private static final IntWritable ZERO = new IntWritable(0);
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String strs[] = value.toString().split("\\s+");
		String myself = strs[0];
		for (int i = 1; i < strs.length; i++) {
			names.set(getKeyForNames(myself, strs[i]));
			context.write(names, ZERO);
		}
		for (int i = 1; i < strs.length; i++) {
			for (int j = i + 1; j < strs.length; j++) {
				names.set(getKeyForNames(strs[i], strs[j]));
				context.write(names, ONE);
			}
		}
	}

	private String getKeyForNames(String s1, String s2) {
		if (s1.compareTo(s2) < 0) {
			return s1 + " " + s2;
		} else {
			return s2 + " " + s1;
		}
	}
}
