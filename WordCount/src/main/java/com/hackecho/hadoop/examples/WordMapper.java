package com.hackecho.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer wordList = new StringTokenizer(value.toString());
		while (wordList.hasMoreTokens()) {
			word.set(wordList.nextToken());
			context.write(word, one);
		}
	}
}
