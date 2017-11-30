package u1525150;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class TopNReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	@Override
	protected void reduce(IntWritable totalRevisions, Iterable<Text> userIds,
			Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		//mb sort in this part, when looping thru userIds?
		for (Text userId : userIds) {
			context.write(userId, totalRevisions);
		}
	}
}