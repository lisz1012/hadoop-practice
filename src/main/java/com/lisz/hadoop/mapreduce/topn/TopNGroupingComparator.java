package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroupingComparator extends WritableComparator {
	// 告诉比较器处理的key1的类型是什么，byte数组读取的时候才好确定长度，见父类中另一个compare方法的实现
	// key1、key2要初始化
	public TopNGroupingComparator() {
		super(TopNkey.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TopNkey t1 = (TopNkey)a;
		TopNkey t2 = (TopNkey)b;
		int c1 = Integer.compare(t1.getYear(), t2.getYear());
		if (c1 == 0) {
			return Integer.compare(t1.getMonth(), t2.getMonth());
		}
		return c1;
	}
}
