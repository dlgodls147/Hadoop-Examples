package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class App {
	public static final String FileName = "hadoop.txt";
	public static final String message = "My First Hadoop API call!\n";

	public static void main(String[] args) throws IOException {
		// Initialize new default Hadoop Configuration
		Configuration conf = new Configuration();
		// Initialize new abstract Hadoop FileSystem
		FileSystem fs = FileSystem.get(conf);
		// Specify File Path of Hadoop File System
		Path filenamePath = new Path(FileName);

		try {
			// Check if file doesn't exist
			if (fs.exists(filenamePath)) {
				// if file exist, remove file first
				fs.delete(filenamePath, true);
			}
			// Write to File
			FSDataOutputStream out = fs.create(filenamePath);
			out.writeUTF(message);
			out.close();

			// Open file to read
			FSDataInputStream in = fs.open(filenamePath);
			String messageIn = in.readUTF();
			System.out.print(messageIn);
			in.close();
		} catch (IOException ioe) {
			System.err.println("IOException during operation: "
					+ ioe.toString());
			System.exit(1);
		}
	}
}