package avrocli.avro;

public class FileArgumentException extends Exception {

	private String message;
	 
    public FileArgumentException(String message){
    	super(message);
        this.message = message;
    }
 
    public String toString(){
        return message;
    }
}
