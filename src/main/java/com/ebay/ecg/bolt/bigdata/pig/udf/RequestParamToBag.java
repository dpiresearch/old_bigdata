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
public class RequestParamToBag extends EvalFunc<DataBag> {

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
			String[] tokens = remainingString.split("\\|&");
			
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
		Tuple t = tupleFactory.newTuple(2);
		if (firstString != null && firstString.length() > 0) {
			int firstStringLength = firstString.length();
			int equalIndex = firstString.indexOf('=');
			
			
			if (equalIndex <= 0) return t;  // if we didn't find anything
			
			String key = firstString.substring(0,equalIndex);		

			String value = "";
			if (equalIndex + 1 >= firstStringLength) { // if we there is no value
				value = "";
			} else {
				value = firstString.substring(equalIndex + 1, firstStringLength);
			}
			
			t.set(0, key);
			t.set(1,value);
		}
		
		return t;
	}

	public static void main(String[] args) {
		String test = "Hello=there|nothing=doing|&go=ahead|&";
		String[] tokens = test.split("\\|&");
		for (String token : tokens) {
			System.out.println(token);
		}
		
	}

}
