package avrocli.avro.mapreduce.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import avrocli.avro.AvroCliConstants;
import avrocli.avro.mapreduce.aggregation.MapAggregation;
import avrocli.avro.mapreduce.aggregation.ReduceAggregation;
import avrocli.avro.mapreduce.count.MapCounter;
import avrocli.avro.mapreduce.distinct.MapDistinct;
import avrocli.avro.mapreduce.distinct.ReduceDistinct;
import avrocli.avro.mapreduce.minmax.ReduceMax;
import avrocli.avro.mapreduce.minmax.ReduceMin;

public class CommonDriver {
	
	public enum Options{
		count, sum, min, max,distinct
	}
	
	public static void trigger(String option,String inputFilePathRegex,String sqlStatement,String argumentsToMapRed,String groupByColumn) throws Exception {
		
		Configuration conf = new Configuration();
    	conf.set("sqlStatement",sqlStatement);
    	conf.set("argumentsToMapRed",argumentsToMapRed);
    	conf.set("groupByColumn", groupByColumn);
    	
	    FileSystem hdfs =FileSystem.get(conf);
	    if(hdfs.exists(new Path(AvroCliConstants.OUTPUT_DIR))) {
	          hdfs.delete(new Path(AvroCliConstants.OUTPUT_DIR), true);
	    }
	    Job job = new Job(conf,"avrocli-jobs-"+option);
	    
	    switch (Options.valueOf(option)) {
	    case count:
		    job.setMapperClass(MapCounter.class);
		    job.setReducerClass(ReduceAggregation.class);
	    	break;
	    case max:
	    	job.setMapperClass(MapAggregation.class);
	    	job.setReducerClass(ReduceMax.class);
	    	break;
	    case min:
	    	job.setMapperClass(MapAggregation.class);
	    	job.setReducerClass(ReduceMin.class);
	    	break;
	    case sum:
	    	job.setMapperClass(MapAggregation.class);
	    	job.setCombinerClass(CommonCombiner.class);
	    	job.setReducerClass(ReduceAggregation.class);
	     	break;
	    case distinct:
	    	job.setMapperClass(MapDistinct.class);
	    	job.setReducerClass(ReduceDistinct.class);	     	
	    }
	    
	    if(!conf.get("groupByColumn").isEmpty()) {
	    	job.setNumReduceTasks(10);
	    }
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setInputFormatClass(AvroFileInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(inputFilePathRegex));
	    FileOutputFormat.setOutputPath(job, new Path(AvroCliConstants.OUTPUT_DIR));
	        
	    if(!job.waitForCompletion(false)){
	    	System.exit(100);
	    }

	}
}
