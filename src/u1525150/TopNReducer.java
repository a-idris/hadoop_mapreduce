package u1525150;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class TopNReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable totalRevisions, Iterable<IntWritable> userIds,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

		//mb sort in this part, when looping thru userIds?
		for (IntWritable userId : userIds) {
			context.write(userId, totalRevisions);
		}
	}
}