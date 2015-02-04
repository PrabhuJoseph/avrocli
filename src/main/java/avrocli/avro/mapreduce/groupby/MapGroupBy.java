package avrocli.avro.mapreduce.groupby;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import avrocli.avro.FilterIntelligence;
import avrocli.cli.SelectCommand;

public class MapGroupBy extends Mapper<GenericRecord, NullWritable, Text, Text> {
		
	    private String mapkey = new String("dummy");
	    private String arguments = "";
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
	    	arguments = conf.get("argumentsToMapRed");
	    }
	    
	    public void map(GenericRecord record, NullWritable value, Context context) throws IOException, InterruptedException {
	    	if(whereCondtionDoNotExists){
	    		context.write(new Text(arguments), new Text(record.get(arguments).toString()));
	    	}
	    	else if(filter.scanRecord(record)){
	    		context.write(new Text(arguments), new Text(record.get(arguments).toString()));
	    	}
	    }
	}
