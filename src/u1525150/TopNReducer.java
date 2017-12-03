package u1525150;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class TopNReducer extends Reducer<IntPair, NullWritable, IntWritable, IntWritable> {
	
	int count = 0;
	int n = Integer.MIN_VALUE;
	
	@Override
	protected void reduce(IntPair compositeKey, Iterable<NullWritable> empty,
			Reducer<IntPair, NullWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		// on first call, set N.
		if (n == Integer.MIN_VALUE) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("N_number", -1);			
			if (n == -1) {
				throw new IOException("invalid N");
			}
		}
		
		//stop writing after n lines have been passed.
		if (count++ < n) {
			IntWritable userId = compositeKey.getSecond();
			IntWritable totalRevisions = compositeKey.getFirst();
			context.write(userId, totalRevisions);				
		}
	}
}