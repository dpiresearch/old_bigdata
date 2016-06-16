package com.ebay.ecg.bolt.bigdata.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: grangaswamy
 * Date: 9/5/14
 * Time: 5:00 PM
 */
public class FilterEmailUsernameFromJson extends EvalFunc<Tuple> {
    private static final List<String> FNS = new ArrayList<String>();

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    static {
        FNS.add("email");
        FNS.add("userName");
    }

    @Override
    public Schema outputSchema(Schema input){
        Schema schema = new Schema();
        for(String fieldName : FNS){
            schema.add(new Schema.FieldSchema(fieldName, DataType.CHARARRAY));
        }
        return schema;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        String rawInput = (String)input.get(0);
        ObjectMapper mapper = new ObjectMapper();
        new ArrayList<Object>();
        LinkedHashMap vals = null;
        try {
            vals = mapper.readValue(rawInput, LinkedHashMap.class);
        } catch (IOException ex) {

        } catch (Exception e) {
            return TupleFactory.getInstance().newTuple();
        }

        new StringBuilder();
        Tuple out = tupleFactory.newTuple();

        for(String key : FNS){
            Object val = vals.get(key);
            if(val != null && !val.toString().isEmpty()){
                out.append(val);
            }else{
                out.append("NOT_AVAILABLE");
            }
        }
        return out;
    }
}