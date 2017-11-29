package u1525150;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class TopNReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int counter = 0;
		Iterator<Text> it = values.iterator();
		while (it.hasNext() && counter++ < 1000) {
			context.write(it.next(), key);
		}
	}
}