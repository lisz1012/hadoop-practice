package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TopMapper extends Mapper<LongWritable, Text, TopNkey, IntWritable> {
	// map可能会被调用多次，定义在外边，减少GC，源码中看到了，map输出的key value是会被序列化进入buffer的
	private TopNkey topNkey = new TopNkey();
	private IntWritable topNVal = new IntWritable();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Calendar calendar = Calendar.getInstance();
	// 前提是dict内容不太多
	private Map<String, String> dict = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		Path path = new Path(cacheFiles[0].getPath()); // cacheFiles[0].getPath() 是/data/input/dict.txt，不能直接用
		BufferedReader br = new BufferedReader(new FileReader(new File(path.getName()))); // 框架把cacheFile放到本地的
		String line = null;
		while ((line = br.readLine()) != null) {
			String strs[] = line.split("\\s+");
			dict.put(strs[0], strs[1]);
		}
		br.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 开发习惯：不要过于自信
		// value： 2019-6-1 22:22:22	1	39
		String s = value.toString();
		String strs[] = s.split("\\s+");
		int temperature = Integer.parseInt(strs[3]);
		try {
			Date date = sdf.parse(strs[0]);
			calendar.setTime(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		topNkey.setYear(calendar.get(Calendar.YEAR));
		topNkey.setMonth(calendar.get(Calendar.MONTH) + 1);
		topNkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
		topNkey.setTemperature(temperature);
		topNkey.setLocation(dict.get(strs[2]));

		topNVal.set(temperature);
		context.write(topNkey, topNVal);
	}
}
