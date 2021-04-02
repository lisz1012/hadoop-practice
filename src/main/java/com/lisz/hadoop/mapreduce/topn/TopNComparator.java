package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNComparator extends WritableComparator {

	// 告诉比较器处理的key1的类型是什么，byte数组读取的时候才好确定长度，见父类中另一个compare方法的实现
	// key1、key2要初始化
	public TopNComparator() {
		super(TopNkey.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		TopNkey t1 = (TopNkey)o1;
		TopNkey t2 = (TopNkey)o2;
		int c1 = Integer.compare(t1.getYear(), t2.getYear());
		if (c1 == 0){
			int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
			if (c2 == 0) {
				return Integer.compare(t2.getTemperature(), t1.getTemperature());
			}
			return c2;
		}
		return c1;
	}
}
