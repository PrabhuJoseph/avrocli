package avrocli.avro.mapreduce.count;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import avrocli.avro.FilterIntelligence;

public class MapCounter extends Mapper<GenericRecord, NullWritable, Text, Text> {

    private final static Text ONE = new Text("1");
    private String word = new String("count");
    private FilterIntelligence filter = null;
    private boolean whereCondtionDoNotExists = false;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	if(conf.get("sqlStatement") == null ){
    		filter = new FilterIntelligence();
    		whereCondtionDoNotExists = true;
    	} else {
    	filter = new FilterIntelligence(conf.get("sqlStatement"));
    	}
    }
    
    public void map(GenericRecord record, NullWritable value, Context context) throws IOException, InterruptedException {
    	if(whereCondtionDoNotExists){
    		context.write(new Text(word), ONE);
    	} else if (filter.scanRecord(record)) {
            context.write(new Text(word), ONE);
    	}
    }
}
