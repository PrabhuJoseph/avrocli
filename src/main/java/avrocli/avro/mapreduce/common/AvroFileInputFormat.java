package avrocli.avro.mapreduce.common;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Common avro util class which read the avro input format record's.
 * 
 * @param <T>
 */

public class AvroFileInputFormat<T> extends FileInputFormat<T, Object> {
	@Override
	public RecordReader<T, Object> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new AvroRecordReader<T>();
	}
}
