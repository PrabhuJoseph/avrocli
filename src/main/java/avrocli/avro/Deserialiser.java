package avrocli.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import avrocli.avro.AvroCliConstants;

public class Deserialiser implements Callable {
 
    private BlockingQueue<StringBuilder> queue;
    
    private String avroData;
    private String fileSystemType;
    private StringBuilder EOQ;
    private String[] columns;
    private String sql;
    
    
    private List<Schema.Field> selectFields= new ArrayList<Schema.Field>();
    private boolean whereClause=true;
    private FilterIntelligence filter = new FilterIntelligence();
    
	private static final Schema.Type Array=Schema.Type.ARRAY;
	private static final String COMBINER="_";
        private static final boolean SUBSTRUCTURE = true;

        public static enum Aggregate {SUM,MIN,MAX};
	
	
	public Deserialiser(String file) throws FileArgumentException, IOException
	{
		setFileSystem(file);
	}
	
     
    public Deserialiser(BlockingQueue<StringBuilder> q, String file, String str, String[] columns,String sqlStmt) throws FileArgumentException, IOException
    {
        this.queue=q;
        this.EOQ=new StringBuilder(str);
        this.columns = (columns==null) ? new String[0] : columns;
        this.sql=sqlStmt;
        setFileSystem(file);
        
    }
  
    public Deserialiser(String file, String sqlStmt) throws FileArgumentException, IOException
    {
        this.sql=sqlStmt;
        setFileSystem(file);
        
    }
    
    private Configuration getConfiguration()
    {
    	Configuration configuration = new Configuration();
	//configuration.set("fs.default.name", "hdfs://"+CRSServers.getHmasterPrivateIP()+":9000");
		return configuration;
    }
    
    private void setFileSystem(String file) throws FileArgumentException, IOException
    {
    	String[] fileSplit = file.split(":");
    	
    	if(fileSplit.length==2)
    	{
    	 if(fileSplit[0].equalsIgnoreCase("file"))
    	 {
    		 fileSystemType = AvroCliConstants.LOCAL_FILESYSTEM;
    		 avroData=fileSplit[1];
    		 
    		 File avro = new File(avroData);
    		 
    		 if(!(avro.exists() && avro.isFile() ))
    		 {
    			 throw new FileArgumentException(file +" File not found in Local File System");
    		 }
    		 
    	 }
    	 else if(fileSplit[0].equalsIgnoreCase("hdfs"))
    	 {
    		 fileSystemType = AvroCliConstants.HDFS_FILESYSTEM;
    		 avroData=fileSplit[1];
    		 
    		 Configuration config = getConfiguration();
    		 FileSystem fs = FileSystem.get(config);
    		 Path avro = new Path(avroData);
    		 if(! ( fs.exists(avro) && fs.isFile(avro) ))
    		 {
    			 throw new FileArgumentException(file +" File not found in HDFS File System");
    		 }
    	 }
    	 else
    	 {
    		throw new FileArgumentException("File System Given is wrong. USAGE: file:<path> or hdfs:<path>");
    	 }
    	 
    	}
    	 else
    	 {
    		throw new FileArgumentException("File Argument Given is wrong. USAGE: file:<path> or hdfs:<path>");
    	 }
    }
    
    
 // Creating DataFileReader connection
    
   public DataFileReader<GenericRecord> getDataFileReader() throws IOException, FileArgumentException 
   {
    		if (fileSystemType.equals(AvroCliConstants.LOCAL_FILESYSTEM)) 
    		{
    			File file = new File(avroData);
    			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
    			return dataFileReader;
    		} 
    		else if (fileSystemType.equals(AvroCliConstants.HDFS_FILESYSTEM)) 
    		{
    			Configuration configuration = getConfiguration();
    			Path path = new Path(avroData);
    			//configuration.set("fs.default.name", "hdfs://"+CRSServers.getHmasterPrivateIP()+":9000");
    			SeekableInput input = new FsInput(path, configuration);
    			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
    			return dataFileReader;
    		}
    		else
    			throw new FileArgumentException("File System Given is wrong. USAGE: file:<path> or hdfs:<path>");
    	
    }
    
    
    // Closing DataFileReader connection
    
    private void closeReader(DataFileReader dataFileReader)
    {
    	try  
    	{ 
    		if(dataFileReader!=null)
    		{
    			dataFileReader.close(); 
    		}
    	} 
    	catch (IOException e)  { }	
    }
    


    
    // Getting High Level Schema of given AVRO file
    
	public Map<String,String> getSchema(boolean substructure) throws IOException, FileArgumentException, InterruptedException 
	{

		Map<String,String> avroSchema = new LinkedHashMap<String,String>();
		 
		StringBuilder csvHeader=new StringBuilder();
		 
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader(); 
		
		try
		{
			
			Schema schema=dataFileReader.getSchema();
			
			Iterator<Schema.Field> iterator=schema.getFields().iterator();

			Schema.Field field=null;
			
			String subSchema = "";
			String splits[] = new String[0];
			String fieldName="";
			String fieldType="";
			
			
			while(iterator.hasNext())
			{
				field=iterator.next();
				
				//System.out.println(field.name()+"-------"+field.schema().getType().name());
				
				avroSchema.put(field.name(),field.schema().getType().name());
				
				csvHeader.append(AvroCliConstants.SEPARATOR).append(field.name());
	
				if(substructure && field.schema().getType().equals(Array)  )
				{
				   
					 subSchema = field.schema().toString();
					
					 splits = subSchema.split("\"name\":");
				
					
					  if(splits.length > 2)
					  {
					    for(int i=2;i<splits.length;i++)
						 {
						 fieldType = splits[i].split("\"type\":")[1];
						 fieldType = fieldType.split(",")[0];
						 fieldName = splits[i].split(",")[0];
						 fieldName = fieldName.substring(1, fieldName.length()-1);
						// System.out.println("MAP="+fieldName.substring(1, fieldName.length()-1)+"----"+fieldType.substring(1, fieldType.length()-1));
						 avroSchema.put(fieldName, fieldType.substring(1, fieldType.length()-1));
						 csvHeader.append(AvroCliConstants.SEPARATOR).append(fieldName);
						 }
					  }
			     }
			}
			
			
		}
		
		finally { closeReader(dataFileReader); }

		return avroSchema;		
	    
	}
	

        public Map<String,String> getSchemaWithFilter() throws IOException, FileArgumentException, InterruptedException 
	{

		Map<String,String> avroSchema = new LinkedHashMap<String,String>();
		 
		StringBuilder csvHeader=new StringBuilder();
		 
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader(); 
		
		try
		{
	

			Schema.Field field=null;
			
			String subSchema = "";
			String splits[] = new String[0];
			String fieldName="";
			String fieldType="";
			
			
	          for(int j=0;j<selectFields.size();j++)
	           {
	        	      
	        	field = selectFields.get(j);
				
				//System.out.println(field.name()+"-------"+field.schema().getType().name());
				
				avroSchema.put(field.name(),field.schema().getType().name());
				
				csvHeader.append(AvroCliConstants.SEPARATOR).append(field.name());
		
	
				if( field.schema().getType().equals(Array)  )
				{
				   
					 subSchema = field.schema().toString();
					
					 splits = subSchema.split("\"name\":");
				
					
					  if(splits.length > 2)
					  {
					    for(int i=2;i<splits.length;i++)
						 {
						 fieldType = splits[i].split("\"type\":")[1];
						 fieldType = fieldType.split(",")[0];
						 fieldName = splits[i].split(",")[0];
						 fieldName = fieldName.substring(1, fieldName.length()-1);
						// System.out.println("MAP="+fieldName.substring(1, fieldName.length()-1)+"----"+fieldType.substring(1, fieldType.length()-1));
						 avroSchema.put(fieldName, fieldType.substring(1, fieldType.length()-1));
						 csvHeader.append(AvroCliConstants.SEPARATOR).append(fieldName);
						
						 }
					  }
			     }
			}

	         queue.put(csvHeader.deleteCharAt(0).append(AvroCliConstants.NEWLINE));
			
		}
		
		finally { closeReader(dataFileReader); }

		return avroSchema;		
	    
	}
	
	
	/*
	
	// Iterating sub structure of schema
	
	private  void iterateSubHeader(final Object object,final String arrayName,final StringBuilder csvHeader,final Map<String,String> avroSchema) throws IOException
	{
	
	
		GenericData.Array<GenericRecord> users = (GenericData.Array<GenericRecord>) object;
		  
        
		String recordName = users.get(0).getSchema().getName();


        List<Schema.Field> fields = users.get(0).getSchema().getFields();
        
        for(int i=0;i< fields.size();i++)
        {
            
        	if(fields.get(i).schema().getType().equals(Array))
    		{
        		
        		iterateSubHeader(fields.get(i),arrayName+COMBINER+recordName+COMBINER+fields.get(i).name(),csvHeader,avroSchema);
        		
    		}
        	else
        	{
        		StringBuffer temp = new StringBuffer();
        		temp.append(arrayName).append(COMBINER).append(recordName).append(COMBINER).append(fields.get(i).name());
        		csvHeader.append(SEPARATOR).append(temp);
        		avroSchema.put(temp.toString(), fields.get(i).schema().getType().getName());
        	
        	}	
        	
        }
	}
	
	
	// Preparing schema of given AVRO file
	
	private Map<String,String> describeHeader(final boolean QUEUE) throws IOException, InterruptedException, FileArgumentException
	{

		Map<String,String> avroSchema=new LinkedHashMap<String,String>();
    	
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader();
		
		  GenericRecord user = null;
		  
		  StringBuilder csvHeader=new StringBuilder();
		  
		  try
		  {
		  
		   if (dataFileReader.hasNext())  
	       {
		    user = dataFileReader.next(user);
	          
	         String fieldName = "";
	          
	         Iterator<Schema.Field> iterator = dataFileReader.getSchema().getFields().iterator(); 
	          
	          while(iterator.hasNext())
	          {
	           
	           Schema.Field field = iterator.next();
	           
	           fieldName = field.name();
	       	   
	       	
	       	   if(field.schema().getType().equals(Array))
	       	   {
	       		   
	       		iterateSubHeader(user.get(fieldName), fieldName, csvHeader, avroSchema);
	     
	       	   }
	       	   else
	       	   {
	       		csvHeader.append(SEPARATOR).append(fieldName);
	       		avroSchema.put(field.name(), field.schema().getType().getName());
	       	   }
	          } 
	          
	          if(QUEUE)
	        	  queue.put(csvHeader.deleteCharAt(0).append(NEWLINE));

	        }
		  
		  }
		
		  finally { closeReader(dataFileReader); }
		  
		  return avroSchema;
	
	}
	

*/


	
	public Map<String,String> sendHeader() throws IOException, InterruptedException, FileArgumentException
	{
            return getSchemaWithFilter();
		//  return describeHeader(!QUEUE);
	}
	
	
	
	// Traversing AVRO records
	
	private void iterateRecord(final Object object,final StringBuilder csvRecord) throws IOException
	{
		GenericData.Array<GenericRecord> users = (GenericData.Array<GenericRecord>) object;
		
		Iterator<GenericRecord> iterator = users.iterator();

                if(!iterator.hasNext())
		  csvRecord.append(AvroCliConstants.SEPARATOR).append("NULL");
		
		
		while( iterator.hasNext() )
		{
			GenericRecord user = iterator.next();
			List<Schema.Field> userRecords = user.getSchema().getFields();
			
			for( int i=0; i<userRecords.size(); i++ )
			{
			  Schema.Field field = userRecords.get(i);
				
			if( field.schema().getType().equals(Array) )
				iterateRecord(user.get(i),csvRecord);
			else
				csvRecord.append(AvroCliConstants.SEPARATOR).append(user.get(i));
			
			}
			
		}
		
  
	}	
		
    
    // Deserialising AVRO records
	
	private  long deserialize(boolean countClause) throws IOException, InterruptedException, FileArgumentException 
	{
		
		long records=0;
		
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader();
		
        StringBuilder csvRecord;
		
	    GenericRecord user = null;
		  
		
		try
		{		  
		// Records iterator
		
		while (dataFileReader.hasNext())     
	    {
	
		   csvRecord = new StringBuilder("");
		   
		   user = dataFileReader.next(user);
           
           Schema.Field field=null;
           
           // where clause
           if( whereClause || filter.scanRecord(user))
           {
     
           // select clause
           
           for(int i=0;i<selectFields.size();i++)
           {
        	      field = selectFields.get(i);
   
            	  if(field.schema().getType().equals(Array))
            		  iterateRecord(user.get(field.name()),csvRecord);
            	  else  
            		  csvRecord.append(AvroCliConstants.SEPARATOR).append(user.get(field.name())); 
            	  
           }
           if(!countClause)
           {
               if(!csvRecord.equals(""))
        	{
        		 queue.put(csvRecord.deleteCharAt(0).append(AvroCliConstants.NEWLINE));
        	}
           }
           else
           {
            records++; 
           }
       //    System.out.println("PUTTING---"+csvRecord);
           
           }

	    }
		 if(!countClause)
		   queue.put(EOQ);
	//	System.out.println("PUTTING---"+EOQ);
		
		}
		
		finally { closeReader(dataFileReader); }
		
		return records; 
	 }

	
	
	// Parsing where statement

	private void prepareWhere(final String sql) throws ColumnNotFoundException, IOException, QueryMalformedException, FileArgumentException,InterruptedException
	{
		if(!sql.equals(""))
		{
		
		filter.parseQuery(sql);
		
		filter.validateColumns(getSchema(!SUBSTRUCTURE));
		 
		whereClause=false;
		}

	}

	
	// parsing select statement
	
	private void prepareSelect() throws IOException, ColumnNotFoundException, FileArgumentException
	{
    	
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader();

	    GenericRecord user = null;
		  
		
		try
		{
		
		Schema schema=dataFileReader.getSchema();
		
		List<Schema.Field> fields=schema.getFields();
		
		
				  
		// Records iterator
		
		if (dataFileReader.hasNext())     
	    {
		   user = dataFileReader.next(user);
           
           Schema.Field field=null;
     
           // Columns iterator
           
            int columnFound=0;
        	for(int i=0;i<columns.length;i++)
        		{
        		 for(int j=0;j<fields.size();j++)
                 {
        		    if( ( columns[i].equals(fields.get(j).name())) || (columns[0].equals("*") && columns.length==1)  )
        			{
        			 selectFields.add(fields.get(j));
        			 columnFound=1;
        			}
        		}
        		if(columnFound==0) 
        			throw new ColumnNotFoundException(columns[i]);
        		columnFound=0;
              }

	     }
		
		}
		
		finally { closeReader(dataFileReader); }
		
	//	System.out.println(selectFields);

	}
	
	
    // counting number of records
    
    public long getCount() throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException
    {
    	 
  	     prepareWhere(sql); 
  	     
  	     return deserialize(true);
  	     
    }

    public Double doAggregate(Aggregate agg,String onColumn) throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException, IllegalAggregateColumnException {
    	
		prepareWhere(sql); 
		
		DataFileReader<GenericRecord> dataFileReader = getDataFileReader();
        GenericRecord user = null;
	    Schema.Field onField=null;
		  
		
	    // Logic to validate sum Field
		try {
		
		Schema schema=dataFileReader.getSchema();
		List<Schema.Field> fields=schema.getFields();

		if (dataFileReader.hasNext()) {
		   user = dataFileReader.next(user);

        	for(int j=0;j<fields.size();j++) {
        		if(onColumn.equals(fields.get(j).name())) {
        			onField = fields.get(j);
        			 break;
        			}
        		}
        	if(onField==null) 
        		throw new ColumnNotFoundException(onColumn);
        	if(onField.schema().getType().equals(Array))
        		throw new IllegalAggregateColumnException(onColumn+" is not a Number");
	     }
		
		}
		
		finally { closeReader(dataFileReader); }
		
		// Logic to calculate sum with filtering
		
		double out = 0.0d;
		double tmp = 0.0d;
		
		dataFileReader = getDataFileReader();
		
		try{
		
		switch(agg.ordinal()) {
		
		case 0:
			
			  while (dataFileReader.hasNext()) {
				    user = dataFileReader.next(user);
		            // where clause
		            if( whereClause || filter.scanRecord(user)) {
		            		tmp = Double.parseDouble(user.get(onField.name()).toString());
		            		out = out + tmp;
		            }
		          }
			
                break;			
		case 1:
			
			  while (dataFileReader.hasNext()) {
				    user = dataFileReader.next(user);
		            // where clause
		            if( whereClause || filter.scanRecord(user)) {
		            		out = Double.parseDouble(user.get(onField.name()).toString());	
		            		break;
		            }
		          }
			 
			  while (dataFileReader.hasNext()) {
				    user = dataFileReader.next(user);
		            // where clause
		            if( whereClause || filter.scanRecord(user)) {
		            		tmp = Double.parseDouble(user.get(onField.name()).toString());
		            		if(tmp<out)
		            			out=tmp;
		            }
		          }	  
			
			    break;
		case 2:
			
			  while (dataFileReader.hasNext()) {
				    user = dataFileReader.next(user);
		            // where clause
		            if( whereClause || filter.scanRecord(user)) {
		            		out = Double.parseDouble(user.get(onField.name()).toString());	
		            		break;
		            }
		          }
			
			  while (dataFileReader.hasNext()) {
				    user = dataFileReader.next(user);
		            // where clause
		            if( whereClause || filter.scanRecord(user)) {
		            		tmp = Double.parseDouble(user.get(onField.name()).toString());
		            		if(tmp>out)
		            			out=tmp;
		            }
		          }
			
			    break;
		default:
			throw new IllegalAggregateColumnException("Invalid Aggregate Operation"); 
		}
			  
		// Records iterator
		
		}
		
		catch(NumberFormatException e){ throw new IllegalAggregateColumnException(onColumn+" is not a Number"); }
		
		finally { closeReader(dataFileReader); }
		
		return out;
    	
    }
    
    
    // Callable Thread which deserializes AVRO file
    
	public Integer call() throws IOException, ColumnNotFoundException, InterruptedException, QueryMalformedException, FileArgumentException
	{

	     prepareSelect();
	     
	     prepareWhere(sql); 
		 	 
	     sendHeader();

	     deserialize(false);
	     
	     return 1;
    
	}




    
    
    
 
}
