package u1525150;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class UserIdMapper extends IdMapper {

	//simple impl
	public void extractAndApply(String[] tokens, Context context) throws IOException, InterruptedException {
		//get user_id
		String userIdStr = tokens[6];
		if (!userIdStr.startsWith("ip")) {
			int userId = Integer.parseInt(userIdStr);
			context.write(new IntWritable(userId), new IntWritable(1));
		}
	}
	
	//in mapper combining impl
	public void processId(String[] tokens, Context context, Map<Integer, Integer> accumulatedRevisions) {
		//get user_id, discarding anonymous ip user_ids
		String userIdStr = tokens[6];
		if (!userIdStr.startsWith("ip")) {
			int userId = 0;
			try {
				userId = Integer.parseInt(userIdStr);
			} catch(NumberFormatException e) {
				return;
			}
			// set revision count to 1 if user_id not in map, else store incremented present value 
			accumulatedRevisions.compute(userId, (uidKey, revisionCount) -> revisionCount == null ? 1 : ++revisionCount);
		}
	}

}
