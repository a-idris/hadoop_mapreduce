package u1525150;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text userId, Iterable<IntWritable> counts,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int totalRevisions = 0;
		for (IntWritable count : counts) {
			totalRevisions += count.get();
		}
		context.write(userId, new IntWritable(totalRevisions));
	}
}