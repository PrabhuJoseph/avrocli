package avrocli.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
 
 
public class Main {
	
	
	public Map<String, String> describe(final String avroName) throws IOException, InterruptedException, FileArgumentException
	{
		 
		 return new Deserialiser(avroName).getSchema(true);
		 
		// return new Deserialiser(avroName).getSchema();
		 
	}
	
	
	public long count(final String sqlStmt, final String avroName) throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException  
	{
	     return new Deserialiser(avroName,sqlStmt).getCount();
	}
	


	public Double sum(final String sumColumn,final String sqlStmt,final String avroName) throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException, IllegalAggregateColumnException {
		 return new Deserialiser(avroName,sqlStmt).doAggregate(Deserialiser.Aggregate.SUM,sumColumn);
	}
	

	public Double min(final String minColumn,final String sqlStmt,final String avroName) throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException, IllegalAggregateColumnException {
		 return new Deserialiser(avroName,sqlStmt).doAggregate(Deserialiser.Aggregate.MIN,minColumn);
	}
	

	public Double max(final String maxColumn,final String sqlStmt,final String avroName) throws IOException, ColumnNotFoundException, QueryMalformedException, InterruptedException, FileArgumentException, IllegalAggregateColumnException {
		 return new Deserialiser(avroName,sqlStmt).doAggregate(Deserialiser.Aggregate.MAX,maxColumn);
	}



	
	public long select(final String[] columns, final String sqlStmt, final String avroName, final String csvName) throws InterruptedException, ExecutionException, FileArgumentException, IOException 
	{
		
		 BlockingQueue<StringBuilder> queue = new ArrayBlockingQueue<StringBuilder>(1024);
		
	        
	     String EOQ="EOQ";
	     
	        
	     Deserialiser producer = new Deserialiser(queue,avroName,EOQ,columns,sqlStmt);
	     CSVWriter consumer = new CSVWriter(queue,csvName,EOQ);

	     ExecutorService producerExecutor = Executors.newSingleThreadExecutor();
	     
	     ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

	         // Start task on another thread
	         Future<Integer> producerResult = producerExecutor.submit(producer);
	         
	         Future<Integer> consumerResult = consumerExecutor.submit(consumer);

	         // Now wait until the result is available
	         int result = producerResult.get();

	         int result1 = consumerResult.get();
	         
	         producerExecutor.shutdown();
	         
	         consumerExecutor.shutdown();
	         
	         if(result==1 && result1==1)
	        	 return consumer.getCount();
	         
	         return -1;
	         
	         
	
	}
	
	
	
	public void bulkAvroToCSV(final String src,final String dest,final int threadCount) throws InterruptedException, ExecutionException, FileArgumentException, IOException
	{

		
		final List<String> avroFiles = new ArrayList<String>();

		
		
		File destDir = new File(dest);
		
		if(!(destDir.exists() && destDir.isDirectory()))
			throw new FileArgumentException(destDir +" Destination Directory not found in Local File System");
		
		final String destdir = dest.endsWith("/")? dest : dest+"/";
		
		
        String[] fileSplit = src.split(":");
    	
    	if(fileSplit.length==2)
    	{
    	 if(fileSplit[0].equalsIgnoreCase("file"))
    	 {
    		   String srcdir = fileSplit[1];
    		   
    		   File srcDir = new File(srcdir);
    		   
    		   if(srcDir.exists() && srcDir.isDirectory())
    		   {
    		   File[] fList = srcDir.listFiles();
    		    
    		    for (File file : fList) 
    		    {
    		        if (file.getName().endsWith(".avro")) 
    		        {
    		        	avroFiles.add("file:"+file.getAbsolutePath());
    		        }
    		    }
    		   }
    		   else
    		   {
    			   throw new FileArgumentException(src+ " Source Directory does not exist in Local File System");
    		   }
    		    
    		 
    	 }
    	 else if(fileSplit[0].equalsIgnoreCase("hdfs"))
    	 {
    			Configuration conf = new Configuration();
    			
        		 Path avro = new Path(fileSplit[1]);
        		 
        		 FileSystem fs = avro.getFileSystem(conf);
        		 
        		 if(( fs.exists(avro) && fs.isDirectory(avro) ))
        		 {
        			 RemoteIterator<LocatedFileStatus> fileArray = fs.listFiles(avro, false);
        			 
        			  while(fileArray.hasNext())
        		      {
        				  LocatedFileStatus lfs = fileArray.next();
        				  
        				  if(lfs.getPath().getName().endsWith(".avro"))
        				  {
        					  avroFiles.add("hdfs:"+new Path(null,null,lfs.getPath().toUri().getPath()).toString());
        				  }
        		      } 
        		 }
        		 else
      		     {
      			   throw new FileArgumentException(src+ " Source Directory does not exist in HDFS File System");
      		     }
    			
    	 }
    	 else
    	 {
    		throw new FileArgumentException("Source File System Given is wrong. USAGE: file:<path> or hdfs:<path>");
    	 }
    	}
    	 else
    	 {
    		throw new FileArgumentException("Source File System Given is wrong. USAGE: file:<path> or hdfs:<path>");
    	 }
    	
		
	 

         int threadPoolSize= threadCount>=10 || threadCount<=0 ? 6 : threadCount;
         
         
	     ExecutorService bulk = Executors.newFixedThreadPool(threadPoolSize);
	     
	     
	     class Task implements Callable 
	     {
	          public Integer call() throws Exception
	          {
	        	  Main main = new Main();
	        	  String avro=null;
	        	  
	        	  while(true)
	        	  {
		    		
	              synchronized(avroFiles)
	        	  {
	        		    if(!avroFiles.isEmpty())
	        		    	avro = avroFiles.remove(0);
	        		    else
	        		    	break; 
	        	  }
	        	  
	              System.out.println(Thread.currentThread().getName()+" Processing "+avro+" ...");
	              
	              String[] avroSplit = avro.split("/");
	              
	              String fileName = avroSplit[avroSplit.length-1];
	              
	        	  main.select(new String[]{"*"}, "",avro, destdir+fileName.replace(".avro", ".csv"));
	        	  
	        	  }
		    		
		    	  return 1;
			}
	    }
	     
	       List<Callable<Integer>> todo = new ArrayList<Callable<Integer>>(threadPoolSize);

	       for (int i=0;i<threadPoolSize;i++)
	       { 
	    	   todo.add(new Task()); 
	       }

	       List<Future<Integer>> results = bulk.invokeAll(todo);
	       
	       
	       for(int i=0;i<results.size();i++)
	       {
	         Integer result = results.get(i).get();
	 
	       }
		   
		   bulk.shutdown();
		
	}
	
	
 
}
