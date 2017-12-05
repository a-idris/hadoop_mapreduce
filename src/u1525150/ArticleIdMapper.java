package u1525150;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;

public class ArticleIdMapper extends IdMapper {
	
	//simple impl
	public void extractAndApply(String[] tokens, Context context) throws IOException, InterruptedException {
		String articleIdStr = tokens[1];
		int articleId = Integer.parseInt(articleIdStr);
		context.write(new IntWritable(articleId), new IntWritable(1));
	}
	
	//in mapper combining impl
	public void processId(String[] tokens, Context context, Map<Integer, Integer> accumulatedRevisions) {
		String articleIdStr = tokens[1];
		int articleId = Integer.parseInt(articleIdStr);
		// set revision count to 1 if user_id not in map, else store incremented present value 
		accumulatedRevisions.compute(articleId, (idKey, revisionCount) -> revisionCount == null ? 1 : ++revisionCount);
	}
}
