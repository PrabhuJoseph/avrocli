package avrocli.avro.mapreduce.minmax;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceMinMax extends Reducer<Text,Text,Text,Text> {
	private static double min = Double.MAX_VALUE;
	private static double max = Double.MIN_VALUE;
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		
		        for (Text val : values) {
		        	double minCondition = Double.parseDouble(val.toString());
		    		    	if(minCondition < min) {
		    		    		min = minCondition; 
		    		    	}
		    		    	
		    		       double maxCondition = Double.parseDouble(val.toString());
		    		       if(maxCondition > max) {
		    		    	   max = maxCondition;
		    		       }
		        }
		        context.write(key, new Text(String.valueOf(min)+";"+String.valueOf(max)));
		    }
	

}
