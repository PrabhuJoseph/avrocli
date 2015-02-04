package avrocli.avro.mapreduce.distinct;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceDistinct extends Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	double sum = 0;
	    	String distinctElement = "";
	    	      for (Text val : values) {
	    	              sum += 1;
	    	              distinctElement = val.toString();
	    	          }
	    	context.write(new Text(distinctElement), new Text(String.valueOf(sum)));
	        }
	}
