package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FindSearchResultOverlap extends EvalFunc<Float> {

	public Float exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0 || input.size() != 2)
			return null;
		
		String inventory1 = (String) input.get(0);  // this is the first search string
		
		String inventory2 = (String) input.get(1);  // this is the second search string
		
		float returnval = calculateOverlap(inventory1, inventory2);
		return returnval;
	}
	
	public static void main(String[] args) {
		String searchStr1 = "EMPTY|10010|10012";
		String searchStr2 = "EMPTY|10010|10012";

		float overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("1 Overlap is "+(overlap) * 100 + "%");

		searchStr1 = "EMPTY|10010|10012";
		searchStr2 = "EMPTY|10010";

		overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("2 Overlap is "+(overlap) * 100 + "%");

		searchStr1 = "EMPTY|10010|10012";
		searchStr2 = "EMPTY|10010|10013";

		overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("3 Overlap is "+(overlap) * 100 + "%");

		searchStr1 = "EMPTY|10010|10012";
		searchStr2 = "";

		overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("4 Overlap is "+(overlap) * 100 + "%");

		searchStr1 = "EMPTY|10010|10012";
		searchStr2 = "EMPTY|";

		overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("5 Overlap is "+(overlap) * 100 + "%");

		searchStr1 = "EMPTY|";
		searchStr2 = "EMPTY|10010|10012";

		overlap = calculateOverlap(searchStr1, searchStr2);
		System.out.println("6 Overlap is "+(overlap) * 100 + "%");
}

	private static float calculateOverlap(String searchStr1, String searchStr2) {
		Set<String> origSet = generateOrigSet(searchStr1);
		
		String[] searchResults2 = parsePipeDelimitedString(searchStr2);
		
		float overlap = calculateOverlap(origSet, searchResults2);
		return overlap;
	}

	/**
	 * Parse the pipe delimited string and return only the adids
	 * 
	 * @param searchStr2
	 * @return
	 */
	private static String[] parsePipeDelimitedString(String searchStr2) {
		String[] searchResults2 = searchStr2.split("\\|");
		if (searchResults2.length < 1) return new String[1];
		return searchResults2;
	}

	public static Set<String> generateOrigSet(String searchStr1) {
		String[] searchResults1 = parsePipeDelimitedString(searchStr1);
		
		Set<String> origSet = new HashSet<String>();
		Collections.addAll(origSet, searchResults1);
		return origSet;
	}
	
	private static float calculateOverlap(Set<String> origSet,
			String[] searchResults2) {
		float overlap = 0;
		for (String lookupStr : searchResults2) {
			if (origSet.contains(lookupStr)) {
				overlap++;
			} else {
				origSet.add(lookupStr);
			}
		}
		return overlap/origSet.size();
	}
}
