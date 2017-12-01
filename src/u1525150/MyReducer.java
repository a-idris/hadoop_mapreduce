package u1525150;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable userId, Iterable<IntWritable> counts,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

		int totalRevisions = 0;
		for (IntWritable count : counts) {
			totalRevisions += count.get();
		}
		context.write(userId, new IntWritable(totalRevisions));
	}
}