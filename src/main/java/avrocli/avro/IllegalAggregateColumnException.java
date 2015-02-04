package avrocli.avro;


public class IllegalAggregateColumnException extends Exception {

	private String message;
	 
    public IllegalAggregateColumnException(String message){
    	super(message);
        this.message = message;
    }
 
    public String toString(){
        return message;
    }
}


