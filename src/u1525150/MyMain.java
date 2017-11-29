package u1525150;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
		Job job =new Job(conf);
		job.setJobName("MyMapReduce");
		job.setJarByClass(MyMain.class);
		
		// declare the mapper, the reducer, the combiner and partitioner to be used.
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setCombinerClass(MyReducer.class);
		//job.setPartitionerClass(MyPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
								
		return job.waitForCompletion(true) ? 0 : 1;
		//don't return, wait until completion.
		//parse outdir files, merging and printing.
		
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyMain(), args));

	}
}