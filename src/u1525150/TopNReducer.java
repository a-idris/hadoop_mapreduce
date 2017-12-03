package u1525150;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class TopNReducer extends Reducer<IntPair, NullWritable, IntWritable, IntWritable> {
	
	int count;
	int n;
	
	@Override
	protected void setup(Reducer<IntPair, NullWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		// on first call, set N.
		Configuration conf = context.getConfiguration();
		n = conf.getInt("N_number", -1);
		count = 0;
	}
	
	@Override
	protected void reduce(IntPair compositeKey, Iterable<NullWritable> empty,
			Reducer<IntPair, NullWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		//stop writing after n lines have been passed.
		if (count++ < n) {
			IntWritable id = compositeKey.getSecond();
			IntWritable totalRevisions = compositeKey.getFirst();
			context.write(id, totalRevisions);				
		}
	}
}