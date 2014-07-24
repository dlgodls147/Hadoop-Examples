package edu.missouri.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeDup {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
			//context.write(value, new Text(""));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("Data Deduplication");

		job.setJarByClass(DeDup.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}