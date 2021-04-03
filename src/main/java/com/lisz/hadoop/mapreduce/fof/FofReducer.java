package com.lisz.hadoop.mapreduce.fof;

import lombok.val;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final IntWritable VALUE = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		int sum = 0;
		while (iterator.hasNext()){
			int value = iterator.next().get();
			if (value > 0) {
				sum += value;
			} else {
				return; // key中的两人是直接好友，根据题意，返回
			}
		}
		VALUE.set(sum);
		context.write(key, VALUE);
	}
}
