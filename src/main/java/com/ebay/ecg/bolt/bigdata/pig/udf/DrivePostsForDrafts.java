package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This udfs takes a bag of {macid,{(macid, draftid, updates, ads, complete_time, post_time)}}
 * and returns {macid,{(draftid, num_posts)}}
 * 
 * @author dapang
 *
 */
public class DrivePostsForDrafts extends EvalFunc<DataBag> {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	
	public DataBag exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0)
			return null;
		
		DataBag macidBag = (DataBag) input.get(0);
		java.util.Iterator<Tuple> postTuples = macidBag.iterator();
		Map<Long, String> draftCompletionMap = new HashMap<Long, String>();
		Map<Long, Integer> postsCount = new HashMap<Long,Integer>();
		SortedSet<Long> completionSet = new TreeSet<Long>();
		completionSet.add(0L);
		
		Set<Long> postTimesSet = new HashSet<Long>();
		// first create the draftCompletion time set
		while (postTuples.hasNext()) {
			Tuple postTuple = (Tuple)postTuples.next();
			postTuple.get(0);
			String theDraftId = (String) postTuple.get(1); // draftid
			postTuple.get(2);
			postTuple.get(3);
			Long draftCompletionTime = (Long) postTuple.get(4); // completion time
			Long postTime = (Long) postTuple.get(5); // post time
			
			if (draftCompletionTime != null) {
				draftCompletionMap.put(draftCompletionTime, theDraftId);
				completionSet.add(draftCompletionTime);
			}
			if (postTime != null) {
				postTimesSet.add(postTime);
			}
		}
		
		Integer notAssignedCount = 0;
		for (Long postTime : postTimesSet) {
			boolean assigned = false;
			for (Long completionTime : completionSet) {
				if (postTime < completionTime) {
					Integer countVal = 0;
					if (postsCount.containsKey(completionTime)) {
						countVal = postsCount.get(completionTime);
					}
					countVal += 1;
					postsCount.put(completionTime, countVal);
					assigned = true;
					break;
				}
			}
			if (!assigned) {
				notAssignedCount++;
			}
		}
		
		// populate the Bag 
		
		DataBag outputBag = bagFactory.newDefaultBag();
		Set<Long> keySet = draftCompletionMap.keySet();
		Iterator<Long> keys = keySet.iterator();
		String draftid = "NONE";
		while (keys.hasNext()) {
			Long completionTime = keys.next();
			draftid = draftCompletionMap.get(completionTime);
			Integer numPosts = postsCount.get(completionTime);
			Tuple t = tupleFactory.newTuple(3);
			t.set(0, draftid);
			t.set(1,completionTime);
			t.set(2, numPosts);
			outputBag.add(t);
		}
		
		Tuple t = tupleFactory.newTuple(3);
		t.set(0, "UNASSIGNED");
		t.set(1, 0L);
		t.set(2, notAssignedCount);
		outputBag.add(t);
		
		return outputBag;
	}
	
	public static void main(String[] args) {
		
	}

}
