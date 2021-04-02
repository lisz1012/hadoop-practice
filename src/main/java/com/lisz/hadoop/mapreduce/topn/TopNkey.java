package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNkey implements Serialization<TopNkey>, WritableComparable<TopNkey> {
	private int year;
	private int month;
	private int day;
	private int temperature;
	private String location;

	public TopNkey() {
	}

	public TopNkey(int year, int month, int day, int temperature) {
		this.year = year;
		this.month = month;
		this.day = day;
		this.temperature = temperature;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	@Override
	public int compareTo(TopNkey that) {
		// 为了让这个案例体现API开发，所以下面的逻辑是一种通用的逻辑：按照时间的正序，
		// 但是我们目前业务需要的是年月温度，且温度倒序，所以一会儿还要开发一个sortComparator
		int c1 = Integer.compare(year, that.year);
		if (c1 == 0){
			int c2 = Integer.compare(month, that.month);
			if (c2 == 0) {
				return Integer.compare(day, that.day);
			}
			return c2;
		}
		return c1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temperature);
		out.writeUTF(location);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		temperature = in.readInt();
		location = in.readUTF();
	}

	@Override
	public boolean accept(Class<?> c) {
		return false;
	}

	@Override
	public Serializer<TopNkey> getSerializer(Class<TopNkey> c) {
		return null;
	}

	@Override
	public Deserializer<TopNkey> getDeserializer(Class<TopNkey> c) {
		return null;
	}
}
