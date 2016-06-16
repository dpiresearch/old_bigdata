package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This udfs takes a bag of {(field1, field2, field3)}
 * and returns (field1, field2,...fieldN), depending on what N is
 * 
 * @author dapang
 *
 */
public class TopNFromBag extends EvalFunc<Tuple> {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	public Tuple exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0)
			return null;
		
		DataBag macidBag = (DataBag) input.get(0);
		java.util.Iterator<Tuple> postTuples = macidBag.iterator();
		Integer n = (Integer) input.get(1);

		String field = "";
		Tuple t = tupleFactory.newTuple(n);
		while (postTuples.hasNext()) { // should only have one
			Tuple postTuple = (Tuple)postTuples.next();
			if (postTuple.size() < n) {
				return t;
			} else {
				for (Integer x=0;x<n;x++) {
					t.set(x,(String) postTuple.get(x));
				}
			}
		}
		return t;
	}
	
	public static void main(String[] args) {
		
	}

}
