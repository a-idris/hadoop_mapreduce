package u1525150;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
		
		Path tempDir = new Path(args[4] + "/temp");
		Path resultDir = new Path(args[4] + "/results");
		
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
		FileOutputFormat.setOutputPath(countJob, tempDir);
								
		boolean countSuccess = countJob.waitForCompletion(true);
		boolean sortSuccess = false;
		
		if (!countSuccess) {
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
			
			sortJob.setSortComparatorClass(IntPairSortComparator.class);
			
			FileInputFormat.addInputPath(sortJob, tempDir);
			FileOutputFormat.setOutputPath(sortJob, resultDir);
			
			sortSuccess = sortJob.waitForCompletion(true);
		} 
		
		if (countSuccess && sortSuccess) {
			//don't return, wait until completion.
			//parse outdir files, merging and printing.
			generateTopN(resultDir, conf);
			return 0; //success
		}
		return 1; //failure
		
	}
	
	public void generateTopN(Path resultDir, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(resultDir.toUri(), conf);
			
			RemoteIterator<LocatedFileStatus> it = fs.listFiles(resultDir, false); 
			List<BufferedReader> resultFiles = new ArrayList<>();
			while (it.hasNext()) {
				LocatedFileStatus fileStatus = it.next();
				FSDataInputStream inStream = fs.open(fileStatus.getPath());
				// convert to buffered reader
				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
				resultFiles.add(br);
			}
			
			merge(fs, resultFiles);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void merge(FileSystem fs, List<BufferedReader> openFiles) {
		FSDataOutputStream resultFile;
		try {
			 resultFile = fs.create(new Path("topN"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//sorted K
		//efficient data structure
		try {
			//init pass
			int max = Integer.MIN_VALUE;
			for (BufferedReader br: openFiles) {
				String line = br.readLine();
				int revisionCount = parseRevisionCount(line);
				if ()
				max = Math.max(max, revisionCount);
			}
		} catch() {
			
		}
	}
	
	private int parseRevisionCount(String record) {
		String[] fields = record.split("\t");
		String revisionCount = fields[1];
		return Integer.parseInt(revisionCount);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyMain(), args));

	}
}