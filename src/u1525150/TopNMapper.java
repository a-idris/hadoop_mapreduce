package u1525150;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class TopNMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, IntWritable, Text>.Context context) 
			throws IOException, InterruptedException {
		context.write(value, key); //not sorted by user_id lexocg. can make composite key (key,value), empty_val. so that it will take care of sorting
	}
}