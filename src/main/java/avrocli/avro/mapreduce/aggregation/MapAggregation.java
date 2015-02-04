package avrocli.avro.mapreduce.aggregation;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import avrocli.avro.FilterIntelligence;

	public class MapAggregation extends Mapper<GenericRecord, NullWritable, Text, Text> {
		
	    private FilterIntelligence filter = null;
	    private boolean whereCondtionDoNotExists = false;
	    private String arguments = "";
	    private String groupByArg = "";
	    private boolean groupByCondition = false;
	    
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	Configuration conf = context.getConfiguration();
	    	if(conf.get("sqlStatement") == null ) {
	    		filter = new FilterIntelligence();
	    		whereCondtionDoNotExists = true;
	    	} else {
	    	filter = new FilterIntelligence(conf.get("sqlStatement"));
	    	}
	    	arguments = conf.get("argumentsToMapRed");
	    	
	    	if(conf.get("groupByColumn") != null) {
	    		groupByCondition = true;
	    		groupByArg = conf.get("groupByColumn");
	    	}
	    }
	    
	    public void map(GenericRecord record, NullWritable value, Context context) throws IOException, InterruptedException {
	    	if(whereCondtionDoNotExists) {
	    		if(groupByCondition) {
	    			context.write(new Text(record.get(groupByArg).toString()), new Text(record.get(arguments).toString()));
	    		} else {
	    	    	context.write(new Text(arguments), new Text(record.get(arguments).toString()));
	    		}
	    	}
	    	else if(filter.scanRecord(record)) {
	    		if(groupByCondition) {
		    		context.write(new Text(record.get(groupByArg).toString()), new Text(record.get(arguments).toString()));
	    		} else {
	    	    	context.write(new Text(arguments), new Text(record.get(arguments).toString()));
	    		}
	    	}
	    }
	}
