package u1525150;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class TopNMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	protected void map(Text userId, IntWritable totalRevisions, Mapper<Text, IntWritable, IntWritable, Text>.Context context) 
			throws IOException, InterruptedException {
		context.write(totalRevisions, userId); //not sorted by user_id lexocg. can make composite key (key,value), empty_val. so that it will take care of sorting
		//possible: complex data type. key = pair (total_revisions, user_id). would need to implement own partitioner.
	}
}