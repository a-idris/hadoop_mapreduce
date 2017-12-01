package u1525150;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) 
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
		
		String line = value.toString();
		//the wanted line and only the wanted line will start with 'REVISION'
		if (line.startsWith("REVISION")) {
			String[] tokens = line.split(" ");
			
			String timestampStr = tokens[4];
			SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
			Timestamp ts = null;
			try {
				ts = new Timestamp(simpleDateFormat.parse(timestampStr).getTime());
			} catch (Exception e) {
				//err handling
				return;
			}
			
			//only pass to reducer if between time bounds
			if (ts.after(startTimestamp) && ts.before(endTimestamp)) {
				//get user_id
				String userIdStr = tokens[6];
				if (!userIdStr.startsWith("ip")) {
					int userId = Integer.parseInt(userIdStr);
					context.write(new IntWritable(userId), new IntWritable(1));
				}
			}
		}
	}
}

/*second mapper after reducer: where key = amount/counter
 * and static counter that emits until #vals_emitted == N
 */
