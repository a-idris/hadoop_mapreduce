package u1525150;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public abstract class IdMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
	Timestamp startTimestamp, endTimestamp;
	Map<Integer, Integer> accumulatedRevisions;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		
		//get bounding timestamps
		long timestamp0 = conf.getLong("start_timestamp", -1);
		startTimestamp = new Timestamp(timestamp0);	
		
		long timestamp1 = conf.getLong("end_timestamp", -1);
		endTimestamp = new Timestamp(timestamp1);
	
		accumulatedRevisions = new HashMap<>();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		//the wanted line starts with 'REVISION'
		if (line.startsWith("REVISION")) {
			String[] tokens = line.split(" ");
			
			String timestampStr = tokens[4];
			SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
			Timestamp ts = null;
			try {
				ts = new Timestamp(simpleDateFormat.parse(timestampStr).getTime());
			} catch (ParseException e) {
				//don't add this record
				return;
			}
			
			//only pass to reducer if between time bounds
			if (ts.after(startTimestamp) && ts.before(endTimestamp)) {
			
				processId(tokens, context, accumulatedRevisions);

			}
		}
	}
	
	// id specific function to apply after other conditions have been filtered
	public abstract void processId(String[] tokens, Context context, Map<Integer, Integer> accumulatedRevisions) throws IOException, InterruptedException;

	@Override
	protected void cleanup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		
		//write the map values
		for (Map.Entry<Integer, Integer> entry : accumulatedRevisions.entrySet()) {
			context.write(new IntWritable(entry.getKey().intValue()), new IntWritable(entry.getValue().intValue())); 
		}
	}	
	
	/*
	//	NAIVE IMPL	 
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
				// id specific func
				extractAndApply(tokens, context);
			}
		}
	}
	
	// id specific function to apply after other conditions have been filtered
	public abstract void extractAndApply(String[] tokens, Context context) throws IOException, InterruptedException;
	*/
}
