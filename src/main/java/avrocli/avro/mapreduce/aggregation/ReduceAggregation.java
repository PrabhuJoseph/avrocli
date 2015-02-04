package avrocli.avro.mapreduce.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceAggregation extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	        String semiColonSeparatedvalues[] = null;
	        double db=0.0;
	        String output = "";
			HashMap map = new HashMap();
	        for (Text val : values) {
	        	
	        	semiColonSeparatedvalues = val.toString().split(";");
				
				for(int i = 0 ; i < semiColonSeparatedvalues.length ; i++) {
					double temp = semiColonSeparatedvalues[i].isEmpty() ? -0.0 : Double.parseDouble(semiColonSeparatedvalues[i]);
					if(map.get("key"+i) ==  null){
						map.put("key"+i, semiColonSeparatedvalues[i]);
					}
					else {
						  
						 db = Double.parseDouble(map.get("key"+i).toString())+temp;
						 map.put("key"+i, db);
					}
				}
	        }
	        Iterator it = map.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pairs = (Map.Entry)it.next();
		        output += String.valueOf(pairs.getValue()+";");
		    }
	        context.write(key, new Text(output));
	    }
	}
