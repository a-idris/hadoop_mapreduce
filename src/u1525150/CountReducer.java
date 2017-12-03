package u1525150;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable id, Iterable<IntWritable> revisionCounts,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

		int totalRevisions = 0;
		for (IntWritable count : revisionCounts) {
			totalRevisions += count.get();
		}
		context.write(id, new IntWritable(totalRevisions));
	}
}