package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * We're converting a parameter map, delineated by | or |& into a 
 * bag of tuples {(key,value), (key,value)...}.
 * 
 * This is so that we can flatten the bag and have one row for every param key-value entry
 * 
 * @author dapang
 *
 */
public class AdFeaturesToBag extends EvalFunc<DataBag> {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	
	public DataBag exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0)
			return null;

		String str = (String) input.get(0);  // this is the paramMap

		DataBag outputBag = bagFactory.newDefaultBag();

		try {
			if (str == null || str.length() == 0) {
				return outputBag;
			}
			
			Integer firstIndex = str.indexOf('|');
			if (firstIndex == -1 || firstIndex == 0) return outputBag;
			
			String firstString = str.substring(0,firstIndex);
			Tuple firstTuple = getTuple(firstString);
			outputBag.add(firstTuple);
			
			String remainingString = str.substring(firstIndex + 1,str.length());
			String[] tokens = remainingString.split("\\|");
			
			if (tokens == null || tokens.length == 0) return outputBag;
			
			for (String token : tokens) {
				Tuple tmpTuple = getTuple(token);
				outputBag.add(tmpTuple);
			}
		} catch (Throwable t) {
			// swallow the throwable for now
			System.out.println("Caught throwable while doing RequestParamToBag");
		}
		
		return outputBag;
	}
	
	private Tuple getTuple(String firstString) throws ExecException {
		Tuple t = tupleFactory.newTuple(8);

		if (firstString != null && firstString.length() > 0) {
			String[] tokens = firstString.split("\\*");
			if (tokens.length == 8) {
				for (int i = 0; i< tokens.length; i++) {
					t.set(i, tokens[i]);
				}
			}
		}
		
		return t;
	}

	public static void main(String[] args) {
		String test = "BUMP_UP*feature.displayname.bumpup*100583701*100442900*null*CREATED*CREATED__BEFORE__INVOICE*100000700|TOP_AD*feature.displayname.topad*100583701*100442901*null*CREATED*CREATED__BEFORE__INVOICE*100000700|URGENT_AD*feature.displayname.urgent*100583701*100442902*null*CREATED*CREATED__BEFORE__INVOICE*100000700|HIGHLIGHT_AD*feature.displayname.highlight*100583701*100442903*null*CREATED*CREATED__BEFORE__INVOICE*100000700";
		String[] tokens = test.split("\\|");
		for (String token : tokens) {
			System.out.println(token);
			String[] fields = token.split("\\*");
			for (String field : fields) {
				System.out.println(field);
			}
		}
		
	}

}
