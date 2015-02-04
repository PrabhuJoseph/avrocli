package avrocli.avro.mapreduce.minmax;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceMin extends Reducer<Text,Text,Text,Text> {
	private static double min = Double.MAX_VALUE;
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
			HashMap map = new HashMap();
			String output = "";
		        for (Text val : values) {
		        	
						String semiColonSeparatedvalues[] = val.toString().split(";");
						
						for(int i = 0;i <semiColonSeparatedvalues.length;i++) {
							if(map.get("key"+i) == null){
								map.put("key"+i, semiColonSeparatedvalues[i]);
							} else {
								double minCondition = map.get("key"+i).toString().isEmpty() ? min : Double.parseDouble(map.get("key"+i).toString());
								double iteratedValue = semiColonSeparatedvalues[i].isEmpty() ? min : Double.parseDouble(semiColonSeparatedvalues[i]);
			    		    	if(minCondition < iteratedValue){
			    		    		map.put("key"+i,minCondition);
			    		    	} else {
			    		    		map.put("key"+i,Double.parseDouble(semiColonSeparatedvalues[i]));
			    		    	}
			    		    	
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
