package avrocli.avro;

public class ColumnNotFoundException extends Exception {

	private String columnName;
	
	 
    public ColumnNotFoundException(String columnName){
    	super(columnName+ " Column Not found");
        this.columnName = columnName;
    }
 
    public String toString(){
        return columnName + " Column Not found" ;
    }
    
}
