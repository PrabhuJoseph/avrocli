package avrocli.avro.mapreduce.common;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonCombiner extends Reducer <Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		super.reduce(key, values, context);
	}

}
