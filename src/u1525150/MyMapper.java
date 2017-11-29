package u1525150;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) 
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		//get bounding timestamps
		long timestamp0 = conf.getLong("start_timestamp", -1);
		if (timestamp0 == -1) {
			throw new IOException();
			
		} 
		Timestamp startTimestamp = new Timestamp(timestamp0);	
		
		long timestamp1 = conf.getLong("end_timestamp", -1);
		if (timestamp1 == -1) {
			throw new IOException();		
		}
		Timestamp endTimestamp = new Timestamp(timestamp1);
		
		String file = value.toString();
		String[] lines = file.split("\n");
		
		for (int i = 0; i < lines.length; i += 14) {
			String[] tokens = lines[i].split(" ");
			
			String timestamp_str = tokens[4];
			SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
			Timestamp ts = null;
			try {
				ts = new Timestamp(simpleDateFormat.parse(timestamp_str).getTime());
			} catch (Exception e) {
				//err handling
				return;
			}
			
			//only pass to reducer if between time bounds
			if (ts.after(startTimestamp) && ts.before(endTimestamp)) {
				//get user_id
				int userId;
				try {
					userId = Integer.parseInt(tokens[6]);
					context.write(new Text(tokens[6]), new IntWritable(1));
				} catch (NumberFormatException e) {
					return;
				}
			}	
		}
	}
}

/*second mapper after reducer: where key = amount/counter
 * and static counter that emits until #vals_emitted == N
 */
