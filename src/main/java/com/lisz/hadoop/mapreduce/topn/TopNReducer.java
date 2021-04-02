package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNReducer extends Reducer<TopNkey, IntWritable, Text, IntWritable> {
	private Text text = new Text();
	private IntWritable intWritable = new IntWritable();
	/*
	对values进行迭代，key会不会变化？会！
	这里的参数values是通过org.apache.hadoop.mapreduce.task.ReduceContextImpl 中的getValues()方法得到的调用
	org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer.getValues()得到的（WrappedReducer中的reduceContext是一个
	ReduceContextImpl 类的对象）。而这个在Reducer.run中返回的values是一个ValueIterable类型的对象，这个对象里面有一个ValueIterator
	类型的对象，而后者的next()方法里面调用了getNextKeyValue(), 而在getNextKeyValue()方法中
	有这么一句：key = keyDeserializer.deserialize(key);改变了key的引用所指向的对象，而在父类Reducer的run中，会调用
	reduce(context.getCurrentKey(), context.getValues(), context); 其中context.getCurrentKey()会把key返回
	ReduceContextImpl中的
	while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    是为了在Reducer.run中调用nextKey()的时候，通过 nextKeyIsSame为true，迅速跳到下一个而不做任何事情空转，什么时候!hasMore或者
    !nextKeyIsSame了，才返回true（或者false），使得继续执行。第一组的reduce方法返回之后，有可能空转，然后再进入第二组的reduce方法。
    相当于是在外层的while循环的循环条件方法里放空，然后到该读的地方继续读取. 框架考虑到了有可能while遍历Iterable没有遍历完，中间break的情况
    数据：
    1970-6-4 33    33
    1970-6-4 32    32
    1970-6-22 31   31
    1970-6-4 22    22

    相同的年月的记录都会进入values
	 */
	@Override
	protected void reduce(TopNkey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Iterator<IntWritable> iterator = values.iterator();
		boolean firstWritten = false;
		int firstDay = key.getDay();
		while (iterator.hasNext()){
			iterator.next();
			if (!firstWritten) {
				text.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getLocation());
				intWritable.set(key.getTemperature());
				context.write(text, intWritable);
				firstWritten = true;
			}
			if (key.getDay() != firstDay) {
				text.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getLocation());
				intWritable.set(key.getTemperature());
				context.write(text, intWritable);
				break;
			}
		}
	}
}
