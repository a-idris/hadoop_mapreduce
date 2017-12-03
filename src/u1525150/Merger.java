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
			merge(n, fs, resultFiles);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void merge(int n, FileSystem fs, List<BufferedReader> openFiles) {
		FSDataOutputStream resultFile = null;
		try {
			 resultFile = fs.create(new Path("topN"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PriorityQueue<FileHead> pq;
		try {
			// create PriorityQueue that orders descendingly the FileHead values, which compare based on revisionCount 
			pq = new PriorityQueue<FileHead>(n, (fh1, fh2) -> -1 * fh1.compareTo(fh2));
			//initial pass
			for (BufferedReader br: openFiles) {
				String line = br.readLine();
				int revisionCount = parseRevisionCount(line);
				FileHead fileHead = new FileHead(revisionCount, line, br); 
				pq.offer(fileHead);
			}
			
			int added = 0;
			while (added++ < n) {
				//get max FileHead (based on revision count) from all the open reducer output files
				FileHead max = pq.poll();
				String recordStr = max.getLine();
				//write it to the final output file
				resultFile.writeBytes(recordStr + "\n");
				
				//update the FileHead for this BufferedReader in the priority queue
				BufferedReader br = max.getBufferedReader();
				String line = br.readLine();
				//the line may be null in case have read the whole output file, which stores only n records. only happens if all topN records are in one output file
				if (line != null) {
					int revisionCount = parseRevisionCount(line);
					pq.offer(new FileHead(revisionCount, line, br));	
				}
			}
			
			resultFile.close();
		} catch(Exception e) {
			e.printStackTrace();
		}

	}
	
	private static int parseRevisionCount(String record) {
		//TextOutputFormat has \t as delimiter
		String[] fields = record.split("\t");
		String revisionCount = fields[1];
		return Integer.parseInt(revisionCount);
	}
}

class FileHead implements Comparable<FileHead>{
	private int revisionCount;
	private BufferedReader bufferedReader;
	private String line;
	
	public FileHead(int revisionCount, String line, BufferedReader br) {
		this.revisionCount = revisionCount;
		this.line = line;
		this.bufferedReader = br;
	}
	
	public int getRevisionCount() {
		return revisionCount;
	}
	
	public BufferedReader getBufferedReader() {
		return bufferedReader; 
	}

	public String getLine() {
		return line;
	}
	
	@Override
	public int compareTo(FileHead fileHead) {
		return Integer.valueOf(revisionCount).compareTo(((FileHead) fileHead).getRevisionCount());
	}
}























