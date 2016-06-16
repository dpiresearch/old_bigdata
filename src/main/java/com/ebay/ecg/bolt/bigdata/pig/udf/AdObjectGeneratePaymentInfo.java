package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * We're taking the payment information we've gathered and formatted it into a form suitable for marketo consumption
 * 
 * @author dapang
 *
 */
public class AdObjectGeneratePaymentInfo extends EvalFunc<Tuple> {

	private static final int NUM_MARKETO_FEATURE_FIELDS = 10;
	private static final String EMPTY_STRING = "";
	private static final int FP_OFFSET = 1;
	private TupleFactory tupleFactory = TupleFactory.getInstance();
			
	public Tuple exec(Tuple tuple) throws IOException {
		
		if (tuple == null || tuple.size() == 0)
			return null;

		DataBag featureBag = (DataBag) tuple.get(0);
		Iterator<Tuple> featureItr = featureBag.iterator();
		
		// Grab every feature with a bag and decide if
		Boolean paymentIncomplete = false;
		Boolean featuresPurchased = false;
		List<String> outputPurchasedList = getDefaultOutputPurchasedList();
		Integer featuresPurchasedTupleIndex = 0; // Index of the outputPurchasedList to be written.
		while(featureItr.hasNext()) {
			Tuple featureTuple = (Tuple) featureItr.next();
			String orderNumber = (String) featureTuple.get(0);
			String featureString = (String) featureTuple.get(1);
			String adId = (String) featureTuple.get(2);
			Integer paymentIncompleteInt = (Integer) featureTuple.get(3);

			if (paymentIncompleteInt == 1) { // Set the payment incomplete flag to true to signal that a feature was not purchased.
				paymentIncomplete = true;
			} else { // if paymentIncomplete is 0, the a feature was purchased.  Record the feature string and signal that a feature was purchased
				if (!outputPurchasedList.contains(featureString)) {
					outputPurchasedList.set(featuresPurchasedTupleIndex, featureString);
					featuresPurchasedTupleIndex++;			
				}
				featuresPurchased = true;
			}
		}
		
		// Start building the tuple
		Tuple outputTuple = tupleFactory.newTuple(12);
		outputTuple.set(0, featuresPurchased.toString());
		for (int i = 0; i < outputPurchasedList.size(); i++) {
			outputTuple.set(i + FP_OFFSET, outputPurchasedList.get(i));
		}
		outputTuple.set(FP_OFFSET + outputPurchasedList.size(), paymentIncomplete.toString());
		return outputTuple;
	}

	private List<String> getDefaultOutputPurchasedList() {
		List<String> featuresPurchasedList = new ArrayList<String>(NUM_MARKETO_FEATURE_FIELDS);
		for (int i = 0;i < 10; i++) {
			featuresPurchasedList.add(EMPTY_STRING);
		}
		
		List<String> outputPurchasedList = featuresPurchasedList; // this gives me a list of ten strings defaulted to empty
		return outputPurchasedList;
	}

	/**
	 * This main routine currently doesn't test anything
	 * @param args
	 */
	public static void main(String[] args) {
		String test = "	(100586103,{(100395300,BUMP_UP,100586103,0),(100395300,BUMP_UP,100586103,0)," +
				"(100395300,BUMP_UP,100586103,0),(100395301,BUMP_UP,100586103,0),(100395301,BUMP_UP,100586103,0)," +
				"(100395301,BUMP_UP,100586103,0),(100395302,BUMP_UP,100586103,0),(100395302,BUMP_UP,100586103,0)," +
				"(100395302,BUMP_UP,100586103,0),(,BUMP_UP,100586103,1),(,TOP_AD,100586103,1),(,URGENT_AD,100586103,1)," +
				"(,HIGHLIGHT_AD,100586103,1),(100395400,BUMP_UP,100586103,0),(100395400,BUMP_UP,100586103,0)," +
				"(100395400,BUMP_UP,100586103,0),(100395401,BUMP_UP,100586103,0),(100395401,BUMP_UP,100586103,0)," +
				"(100395401,BUMP_UP,100586103,0),(100395402,BUMP_UP,100586103,0),(100395402,BUMP_UP,100586103,0)," +
				"(100395402,BUMP_UP,100586103,0),(100395403,BUMP_UP,100586103,0),(100395403,BUMP_UP,100586103,0)," +
				"(100395403,BUMP_UP,100586103,0),(100395404,BUMP_UP,100586103,0),(100395404,BUMP_UP,100586103,0)," +
				"(100395404,BUMP_UP,100586103,0)})";
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
