package u1525150;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyMain extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		//conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
		//conf.set("mapred.jar", "/hadoop_workspace/example.jar");
		
		// deal with the arguments from command line
		int N = Integer.parseInt(args[0]);
		SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		long ts0=simpleDateFormat.parse(args[1]).getTime();
		long ts1=simpleDateFormat.parse(args[2]).getTime();
		
		// store the arguments to the configuration, then each mapper and reducer can get access to them
		conf.setLong("start_timestamp",ts0);
		conf.setLong("end_timestamp",ts1);
		conf.setInt("N_number",N);
		
		// create the a job to run the code
		@SuppressWarnings("deprecation")
		Job countJob =new Job(conf);
		countJob.setJobName("MyMapReduce.count");
		countJob.setJarByClass(MyMain.class);
		
		// declare the mapper, the reducer, the combiner and partitioner to be used.
		countJob.setMapperClass(MyMapper.class);
		countJob.setReducerClass(MyReducer.class);
		//job.setCombinerClass(MyReducer.class);
		//job.setPartitionerClass(MyPartitioner.class);
		
		countJob.setInputFormatClass(TextInputFormat.class);
		countJob.setOutputKeyClass(IntWritable.class);
		countJob.setOutputValueClass(IntWritable.class);
		countJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(countJob, new Path(args[3]));
		Path tempDir = new Path(args[4] + "/temp");
		FileOutputFormat.setOutputPath(countJob, tempDir);
								
		boolean countSuccess = countJob.waitForCompletion(true);
		boolean sortSuccess = false;
		
		if (countSuccess) {
			countJob.isSuccessful();
			@SuppressWarnings("deprecation")
			Job sortJob = new Job(new Configuration(getConf()));
			sortJob.setJobName("MyMapReduce.sort");
			sortJob.setJarByClass(MyMain.class);
			
			sortJob.setMapperClass(TopNMapper.class);
			sortJob.setReducerClass(TopNReducer.class);
			
			//the output of the reducer will be of KeyValueTextInputFormat
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			// map outputs and reducer outputs don't match
//			sortJob.setMapOutputKeyClass(IntWritable.class);
//			sortJob.setMapOutputValueClass(IntWritable.class);
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(IntWritable.class);
			sortJob.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(sortJob, tempDir);
			Path resultDir = new Path(args[4] + "/results");
			FileOutputFormat.setOutputPath(sortJob, resultDir);
			
			sortSuccess = sortJob.waitForCompletion(true);
		} 
		
		if (countSuccess && sortSuccess) {
			//don't return, wait until completion.
			//parse outdir files, merging and printing.
			return 0; //success
		}
		return 1; //failure
		
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyMain(), args));

	}
}