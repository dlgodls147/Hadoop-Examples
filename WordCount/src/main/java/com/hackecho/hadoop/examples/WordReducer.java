package com.hackecho.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable totalWordCount = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<IntWritable> it = values.iterator();
		
		/* actually here we don't have to use iterator
		 * for (IntWritable val : values) {
		 *     wordCount += val.get();
		 * }
		 */
		while (it.hasNext()) {
			wordCount += it.next().get();
		}
		
		totalWordCount.set(wordCount);
		context.write(key, totalWordCount);
	}
}
