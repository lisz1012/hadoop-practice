package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	// hadoop框架中，他还是个分布式的，数据要序列化和反序列化，hadoop的类型对java类型做了包装：String -> Text int -> IntWriteable
	// 也可以自己开发类型，但必须实现hadoop的WritableComparable序列化反序列化接口、比较器接口
	// 排序 -> 比较世界上有两种顺序：字典序和数值序：8， 11到底谁大？
	// 大部分时间重写map方法，偶尔重写setup和cleanup
	// 可以写成成员变量，这是因为：context.write(word, one);这里每次执行都是拿着word和one为模版做序列化
	// 每次序列化出来的字节数组都是新的，互不影响，map后面有一个基于内存的buffer，是一个字节数组
	// 写在这里不用每次都new，这时候性能就节省下来了，不会对GC造成压力
	// 见MapTask的1156行：keySerializer.serialize(key); 然后会将序列化之后的结果放入buffer
	/*
	final float spillper =
        job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
      final int sortmb = job.getInt(MRJobConfig.IO_SORT_MB,
          MRJobConfig.DEFAULT_IO_SORT_MB);
      溢写百分比和buffer的大小将来是要调整的
      默认的Buffer内的排序是快排：
      sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
            QuickSort.class, IndexedSorter.class), job);

        Combiner的设置有时候很重要，自己可以设置
	 */

	// MapTask 794行这两个缓冲区的设置将来可以调整
	// final float spillper =
	//        job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
	//      final int sortmb = job.getInt(MRJobConfig.IO_SORT_MB,
	//          MRJobConfig.DEFAULT_IO_SORT_MB);
	private final static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	//
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	while (itr.hasMoreTokens()) {
    		word.set(itr.nextToken());
    		context.write(word, ONE);
    	}
    }
}
