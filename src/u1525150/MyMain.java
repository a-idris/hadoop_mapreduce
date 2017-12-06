package u1525150;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//class UserRevisionRank
//class ArticleRevisionRank

public class MyMain extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String id = getConf().get("id");
		if (id == null)
			throw new IllegalArgumentException("-D id must be set to either 'user' or 'article'");
		
		long startTime, stopTime;
		int exitCode = 1;
		
		if (id.equals("user")) {
			startTime = System.currentTimeMillis();
			exitCode = runJob(args, UserIdMapper.class);
			stopTime = System.currentTimeMillis();
			System.out.format("Time taken: %d ms\n", stopTime - startTime);
		} else if (id.equals("article")) {
			startTime = System.currentTimeMillis();
			exitCode = runJob(args, ArticleIdMapper.class);
			stopTime = System.currentTimeMillis();
			System.out.format("Time taken: %d ms\n", stopTime - startTime);
		}
		return exitCode;
	}
	
	public int runJob(String[] args, Class<? extends IdMapper> jobMapperClass) throws Exception {
		Configuration conf = new Configuration(getConf());
		//conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml"));
//		conf.set("wiki_mapred.jar", "//example.jar");
		
		// deal with the arguments from command line
		int n = Integer.parseInt(args[0]);
		if (n <= 0) {
			return 0; //no need to process process if 0 length output(FileOutupt);
		}
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		long ts0 = simpleDateFormat.parse(args[1]).getTime();
		long ts1 = simpleDateFormat.parse(args[2]).getTime();
		// store the arguments to the configuration, then each mapper and reducer can get access to them
		conf.setLong("start_timestamp", ts0);
		conf.setLong("end_timestamp", ts1);
		conf.setInt("N_number", n);
		
		Path tempDir = new Path(args[4] + "/temp");
		Path resultDir = new Path(args[4] + "/results");
		
		// create the a job to run the code
		@SuppressWarnings("deprecation")
		Job countJob =new Job(conf);
		countJob.setJobName("MyMapReduce.count");
		countJob.setJarByClass(MyMain.class);
		
		// declare the mapper, the reducer to be used
		countJob.setMapperClass(jobMapperClass);
		countJob.setReducerClass(CountReducer.class);
		
		countJob.setInputFormatClass(TextInputFormat.class);
		countJob.setOutputKeyClass(IntWritable.class);
		countJob.setOutputValueClass(IntWritable.class);
		//use SequenceFileOutputFormat for chaining
		countJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(countJob, true);
		SequenceFileOutputFormat.setOutputCompressionType(countJob, CompressionType.BLOCK);
		
		FileInputFormat.addInputPath(countJob, new Path(args[3]));
		SequenceFileOutputFormat.setOutputPath(countJob, tempDir);
		
								
		boolean countSuccess = countJob.waitForCompletion(true);
		boolean sortSuccess = false;
		
		if (countSuccess) {
			countJob.isSuccessful();
			@SuppressWarnings("deprecation")
			Job sortJob = new Job(conf);
			sortJob.setJobName("MyMapReduce.sort");
			sortJob.setJarByClass(MyMain.class);
			
			sortJob.setMapperClass(TopNMapper.class);
			sortJob.setReducerClass(TopNReducer.class);
			
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			// map outputs and reducer outputs don't match
			sortJob.setMapOutputKeyClass(IntPair.class);
			sortJob.setMapOutputValueClass(NullWritable.class);
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(IntWritable.class);
			sortJob.setOutputFormatClass(TextOutputFormat.class);
			
			sortJob.setSortComparatorClass(IntPairSortComparator.class);
			
			FileInputFormat.addInputPath(sortJob, tempDir);
			FileOutputFormat.setOutputPath(sortJob, resultDir);
			
			sortSuccess = sortJob.waitForCompletion(true);
		} 
		
		if (countSuccess && sortSuccess) {
			//parse resultdir files, and generate topN file
			Merger.generateTopN(resultDir, conf);
			return 0; //success
		}
		return 1; //failure
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyMain(), args));
	}
}
