package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Created with IntelliJ IDEA.
 * User: grangaswamy
 * Date: 4/28/14
 * Time: 11:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class FilterJsonFieldNames extends EvalFunc<Tuple> {
    private static final String DELIMITER = ",";
    private static final List<String> FNS = new ArrayList<String>();

    private final static TupleFactory tupleFactory = TupleFactory.getInstance();

    static {
        FNS.add("locationId");
        FNS.add("categoryId");
        FNS.add("Title");
        FNS.add("Description");
        FNS.add("ForSaleBy");
        FNS.add("Phone");
        FNS.add("UserName");
        FNS.add("Price");
        FNS.add("currencyValues");
        FNS.add("Address");
    }


    @Override
    public Schema outputSchema(Schema input){
        Schema schema = new Schema();
        for(String fieldName : FNS){
            schema.add(new Schema.FieldSchema(fieldName, DataType.INTEGER));
        }
        return schema;
    }
    
    @SuppressWarnings("rawtypes")
	@Override
    public Tuple exec(Tuple input) throws IOException {
        String rawInput = (String)input.get(0);
        ObjectMapper mapper = new ObjectMapper();
        List<String> fieldNames = new ArrayList<String>();
        LinkedHashMap vals = null;
        try {
            vals = mapper.readValue(rawInput, LinkedHashMap.class);
        } catch (IOException ex) {

        } catch (Exception e) {
            return TupleFactory.getInstance().newTuple();
        }

        LinkedHashMap<String, Integer> fieldExists = new LinkedHashMap<String,Integer>();
        for(String f : FNS){
            fieldExists.put(f,0);
        }

        StringBuilder resp = new StringBuilder();
        if(vals != null){
            for(Object key : vals.keySet()){
                Object val = vals.get(key);
                if(val != null && !val.toString().isEmpty()){
                    //Special treatment for postAdParamMap field
                    if("postAdParamMap".equalsIgnoreCase(key.toString())){
                        LinkedHashMap paramMap = (LinkedHashMap) val;
                        for(Object paramKey : paramMap.keySet()){
                            Object paramVal = paramMap.get(paramKey);
                            if(paramVal != null && !paramVal.toString().isEmpty()){
                                if(FNS.contains(paramKey)){
                                    fieldExists.put(paramKey.toString(), 1);
                                }
                                fieldNames.add(paramKey.toString());
                                resp.append(paramKey.toString()).append(DELIMITER);
                            }
                        }
                    }
//                    else{
//                        fieldNames.add(key.toString());
//                        resp.append(key.toString()).append(DELIMITER);
//                    }
                }
            }
        }

        TupleFactory tf = TupleFactory.getInstance();
        Tuple t = tf.newTuple();

        for(String fname : fieldExists.keySet()){
            t.append(fieldExists.get(fname));
        }

        return t;

    }


}

