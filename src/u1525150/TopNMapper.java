package u1525150;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class TopNMapper extends Mapper<IntWritable, IntWritable, IntPair, NullWritable> {
	
	//naive
	
/*	protected void map(IntWritable userId, IntWritable totalRevisions, Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context) 
			throws IOException, InterruptedException {
		//not sorted by user_id lexocg. can make composite key (key,value), empty_val. so that it will take care of sorting
		//possible: complex data type. key = pair (total_revisions, user_id). would need to implement own partitioner.
		//intpair sorts by first IntWritable in descending order and then second IntWritable in ascending order
		IntPair intPair = new IntPair(totalRevisions, userId);
		context.write(intPair, NullWritable.get());
	}*/
	
	PriorityQueue<Integer> topNFound;
	int n;
		
	@Override
	protected void setup(Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		n = conf.getInt("N_number", -1);
		topNFound = new PriorityQueue<Integer>(n);
		topNFound.offer(0); // add value to not need null check in map
	}

	protected void map(IntWritable userId, IntWritable totalRevisions, Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context) 
			throws IOException, InterruptedException {		
		//only consider processing further if totalRevisions is greater than the top N revision counts encountered so far
		if (totalRevisions.get() >= topNFound.peek().intValue()) {
			if (topNFound.size() == n) {
				topNFound.poll(); //remove smallest element
			}
			topNFound.offer(Integer.valueOf(totalRevisions.get()));
			
			//intpair sorts by first IntWritable in descending order and then second IntWritable in ascending order
			IntPair intPair = new IntPair(totalRevisions, userId);
			context.write(intPair, NullWritable.get());	
		}
	}
	
/*	@Override
	protected void cleanup(Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);

		
		
	}*/
}