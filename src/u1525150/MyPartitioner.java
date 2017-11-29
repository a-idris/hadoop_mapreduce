package u1525150;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.*;

public class MyPartitioner extends Partitioner<Text, IntWritable> {
	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}
}
