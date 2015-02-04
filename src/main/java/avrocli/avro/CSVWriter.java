package avrocli.avro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
 
public class CSVWriter implements Callable
{
 
private BlockingQueue<StringBuilder> queue;
private File csvFile;
private String EOQ;
private long records=-2;
     
    public CSVWriter(BlockingQueue<StringBuilder> q, String file, String str)
    {
        this.queue=q;
        this.csvFile=new File(file);
        this.EOQ=str;
    }

    public long getCount()
    {
    	return records;
    }

	public Object call() throws IOException, InterruptedException 
	{
 
        
	    FileWriter fw = null;
	    
	    BufferedWriter bw = null;
	    
        try{
        	
        	csvFile.createNewFile();
        	
        	fw = new FileWriter(csvFile);
      	    
      	    bw = new BufferedWriter(fw);
        	
    	    String record = "";
    	    
    	      while(true)
    	      {
    	       record = queue.take().toString();
    	       //System.out.println(Thread.currentThread().getName()+"TAKING---"+record);
    	       records++;
    	       
    	       if(record.equals(EOQ))
    	    	   break;
    	       bw.write(record);
    	       
    	      }   
    	      
    	      
            
        }

        finally
        {
        	try { if(bw!=null) bw.close(); } catch (IOException e) { }
			
        }
		return 1;
	}
    
    
    
}
