package u1525150;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class TopNMapper extends Mapper<IntWritable, IntWritable, IntPair, NullWritable> {
	
	PriorityQueue<Integer> topNFound;
	int n;
		
	@Override
	protected void setup(Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		n = conf.getInt("N_number", -1);
		topNFound = new PriorityQueue<Integer>(n);
		topNFound.offer(0); // add value to not need null check in map when accessing the min element
	}

	protected void map(IntWritable id, IntWritable totalRevisions, Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context) 
			throws IOException, InterruptedException {		
		//only consider processing further if totalRevisions is greater than the top N revision counts encountered so far
		if (totalRevisions.get() >= topNFound.peek().intValue()) {
			if (topNFound.size() == n) {
				topNFound.poll(); //remove smallest element
			}
			topNFound.offer(Integer.valueOf(totalRevisions.get()));
			
			//intpair sorts by first IntWritable arg in descending order and then by second IntWritable in ascending order
			IntPair revisionIdPair = new IntPair(totalRevisions, id);
			//NullWritable as value since don't need any more info
			context.write(revisionIdPair, NullWritable.get());	
		}
	}
	
	/*
	//naive
	protected void map(IntWritable id, IntWritable totalRevisions, Mapper<IntWritable, IntWritable, IntPair, NullWritable>.Context context) 
			throws IOException, InterruptedException {
		//intpair sorts by first IntWritable arg in descending order and then by second IntWritable in ascending order
		IntPair revisionIdPair = new IntPair(totalRevisions, id);
		context.write(revisionIdPair, NullWritable.get());
	}
	*/
}