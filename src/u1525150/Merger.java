package u1525150;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class Merger {
	
	public static void generateTopN(Path resultDir, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(resultDir.toUri(), conf);
			
			RemoteIterator<LocatedFileStatus> it = fs.listFiles(resultDir, false); 
			//create list of BufferedReaders from all the reducer output files to be able manipulate them
			List<BufferedReader> resultFiles = new ArrayList<>();
			while (it.hasNext()) {
				LocatedFileStatus fileStatus = it.next();
				//skip the "_SUCCESS" file, only need the reducer output files of the farm "part*"
				if (fileStatus.getPath().getName().equals("_SUCCESS")) 
					continue;
				// get the file stream and wrap in buffered reader
				FSDataInputStream inStream = fs.open(fileStatus.getPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
				resultFiles.add(br);
			}
			
			int n = conf.getInt("N_number", 0);
			merge(n, fs, resultDir, resultFiles);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void merge(int n, FileSystem fs, Path parentDir, List<BufferedReader> openFiles) {
		FSDataOutputStream resultFile = null;
		try {
			 resultFile = fs.create(new Path(parentDir, "topN"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PriorityQueue<FileHead> pq;
		try {
			// create PriorityQueue that orders descendingly the FileHead values, which compare based on revisionCount 
			pq = new PriorityQueue<FileHead>(n); //, (fh1, fh2) -> fh1.compareTo(fh2)
			//initial pass
			for (BufferedReader br: openFiles) {
				String line = br.readLine();
				if (line == null) {
					//empty file
					br.close();
					continue;
				}
				String[] fields = line.split("\t");
				int id = Integer.valueOf(fields[0]);
				int revisionCount = Integer.valueOf(fields[1]);
				FileHead fileHead = new FileHead(id, revisionCount, br); 
				pq.offer(fileHead);
			}
			
			int added = 0;
			while (added++ < n && !pq.isEmpty()) {
				//get max FileHead (based on revision count) from all the open reducer output files
				FileHead max = pq.poll();
				if (max == null)
					// in case N exceeds the total number of kv pairs accross all the reducers, return what is written.
					continue;
				String recordStr = max.getLine();
				//write it to the final output file
				resultFile.writeBytes(recordStr + "\n");
				
				//update the FileHead for this BufferedReader in the priority queue
				BufferedReader br = max.getBufferedReader();
				String line = br.readLine();
				//the line may be null in case have read the whole output file, which stores only n records. only happens if all topN records are in one output file
				if (line != null) {
					String[] fields = line.split("\t");
					int id = Integer.valueOf(fields[0]);
					int revisionCount = Integer.valueOf(fields[1]);
					pq.offer(new FileHead(id, revisionCount, br));	
				}
			}
			
			//clean up
			resultFile.close();
			for (BufferedReader br: openFiles) {
				br.close();
			}
		} catch(Exception e) {
			e.printStackTrace();
		} /*finally {
			//clean up
			resultFile.close();
			for (BufferedReader br: openFiles) {
				br.close();
			}
		}*/
	}
}

class FileHead implements Comparable<FileHead>{
	private int id;
	private int revisionCount;
	private IntPair revisionIdPair;
	private BufferedReader bufferedReader;
	
	public FileHead(int id, int revisionCount, BufferedReader br) {
		this.id = id;
		this.revisionCount = revisionCount;
		this.revisionIdPair= new IntPair(revisionCount, id);
		this.bufferedReader = br;
	}
	
	public IntPair getRevisionIdPair() {
		return revisionIdPair;
	}
	
	public int getRevisionCount() {
		return id;
	}
	
	public BufferedReader getBufferedReader() {
		return bufferedReader; 
	}

	public String getLine() {
		return id + "\t" + revisionCount;
	}
	
	@Override
	public int compareTo(FileHead fileHead) {
		//use IntPair compareTo, which sorts descendingly on revision count then ascendingly on id
		return revisionIdPair.compareTo(fileHead.getRevisionIdPair());
	}
}