package u1525150;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class TopNMapper extends Mapper<IntWritable, IntWritable, IntPair, NullWritable> {
	
	protected void map(IntWritable userId, IntWritable totalRevisions, Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context) 
			throws IOException, InterruptedException {
		//context.write(totalRevisions, userId); 
		//not sorted by user_id lexocg. can make composite key (key,value), empty_val. so that it will take care of sorting
		//possible: complex data type. key = pair (total_revisions, user_id). would need to implement own partitioner.
		IntPair intPair = new IntPair(totalRevisions, userId);
		context.write(intPair, NullWritable.get());
		// if (limit = config. );
		//		count++;
	}
}