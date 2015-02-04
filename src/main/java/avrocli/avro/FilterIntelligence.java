package avrocli.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.LinkedListMultimap;

public class FilterIntelligence 
{
	
	public  FilterIntelligence() {
	}
	
	public FilterIntelligence(String sql) {
		try{
			parseQuery(sql);
		} catch(QueryMalformedException e) {
			e.printStackTrace();
		}
	}
	
	//tightly coupled don't change the order
    private static final String[] definedOps = new String[]{"!=",">=","<=",">","<","="}; 
      
    
	public LinkedListMultimap<String,String> conditions = LinkedListMultimap.create();
	
	public List<Integer> operators=new ArrayList<Integer>();
	
	public List<String> andOr = new ArrayList<String>();
	
	
	public final void parseQuery(String sql) throws QueryMalformedException
	{
       // String sql="where a=2 And b>=3 Or a=4 and b<3";
		
		//Query Parser
		
		String[] sqlParts = sql.split(" ");
		
		String[] conditionParts = new String[0];
	
		
		sqlParts[0].equalsIgnoreCase("where");
		
		
		for(int i=1;i<sqlParts.length;i++)
		{
		   int j=0;
		   
			for(;j<definedOps.length;j++)
			{
			 conditionParts = sqlParts[i].split(definedOps[j]);
				
			 if(conditionParts.length==2)
			 {
				conditions.put(conditionParts[0], conditionParts[1]);
				operators.add(j);
				break;
			 }
			}
			if(j==definedOps.length)
			{
				throw new QueryMalformedException("Query Malformed Exception");  
			}
			 	
			i++;
			
			if(i<sqlParts.length)
			{		
			 if( (sqlParts[i].equalsIgnoreCase("and") || sqlParts[i].equalsIgnoreCase("or")) && (i!=sqlParts.length-1)) 
		
			 {
			 andOr.add(sqlParts[i]);
			 }
			 else
			 {
				    throw new QueryMalformedException("Query Malformed Exception");  
				
			 }
			
			}
		
		}		

		
	/*	for(Map.Entry<String, String> cond : conditions.entries())
			System.out.println(cond.getKey()+"---"+cond.getValue());
		
		Iterator iterator=andOr.iterator();
		
		while(iterator.hasNext())
			System.out.println(iterator.next());  */
	}
	
	
	public void validateColumns(Map<String,String> schema ) throws ColumnNotFoundException
	{
       Object[] tableSchema = schema.keySet().toArray();
		
		for(Map.Entry<String, String> cond : conditions.entries())
		{
			int i=0;
			for(;i<tableSchema.length;i++)
			{
				//System.out.println(cond.getKey()+"------"+tableSchema[i]);
				if(cond.getKey().equalsIgnoreCase(tableSchema[i].toString()))
				{ break; } 
			}
			if(i==tableSchema.length)
			{
				throw new ColumnNotFoundException(cond.getKey());
			}
					
		}
		
	}
	
	
	
	
	public boolean scanRecord(GenericRecord user)
	{

		boolean[] results = new boolean[conditions.size()];
		boolean finalResult=true;
		

		String recordValue="";
		String askedValue="";
		
       	int i=0;
	    
	       	for(Map.Entry<String, String> entry: conditions.entries())
	       	{
	   
	       		recordValue=user.get(entry.getKey())==null ? "null":user.get(entry.getKey()).toString();
	            askedValue=entry.getValue();
	            
	           // System.out.println("COMP "+entry.getKey()+"---"+recordValue+"----"+askedValue+"--"+definedOps[operators.get(i)]);
	       		
	           switch (operators.get(i))
	       		{
	       		// != operation
	            case 0:   results[i] = !recordValue.equalsIgnoreCase(askedValue);
	                      break;
	            // >= operation
	            case 1:   results[i] = Integer.parseInt(recordValue) >= Integer.parseInt(askedValue);
	                      break;
	            // <= operation
	            case 2:  results[i] = Integer.parseInt(recordValue) <= Integer.parseInt(askedValue);
	                      break;
	            // > operation
	            case 3:  results[i] = Integer.parseInt(recordValue) > Integer.parseInt(askedValue);
	                     break;
	            // < operation
	            case 4:  results[i] = Integer.parseInt(recordValue) < Integer.parseInt(askedValue);
	                     break;
	            // = operation
	            case 5:  results[i] = recordValue.equalsIgnoreCase(askedValue);
	                      break;
	            

	            }
	       		i++;
	       		
	       	}
	       	
	       	if(conditions.size()==1)
	       	{
	       	//	System.out.println(results[0]+" for record");
	       		return results[0];
	       	}
	       	else
	       		finalResult=results[0];
	       	
	       	for(int j=1;j<results.length;j++)
	       	{
	       	     	if(andOr.get(j-1).equalsIgnoreCase("and"))
		        	{	
	       	     	finalResult = finalResult && results[j]; 
		        	}
		        	else if(andOr.get(j-1).equalsIgnoreCase("or"))
		        	{
		        		finalResult = finalResult || results[j]; 
		        	}
	       	}
	       
	        
	    //  System.out.println(finalResult+" for record");
	       	

		return finalResult;
	}
	
	
	
	

}
