package avrocli.avro;

public class QueryMalformedException extends Exception 
{


	private String message;
	 
    public QueryMalformedException(String message){
    	super(message);
        this.message = message;
    }
 
    public String toString(){
        return message;
    }
}
