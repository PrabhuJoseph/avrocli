package avrocli.cli;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.FileArgumentException;
import avrocli.avro.Main;

public class BulkCommand implements Command 

{
	
    private static final String NAMESPACE = "syscmd";
    private static final String CMD_NAME = "bulkAvroToCsv";
 

	public void plug(Context ctx) {
		
	}


	public Descriptor getDescriptor() {
		
		return new Command.Descriptor() {
			
			public String getUsage() {
				return "\nUSAGE: bulkAvroToCsv <hdfs:/file:/ Avro SRC dir> <CSV DEST dir> <threadCount>\n";
			}
			
			public String getNamespace() {
				return NAMESPACE;
			}
			
			public String getName() {
				return CMD_NAME;
			}
			
			public String getDescription() {
				return "Converts AVRO files to CSV files.";
			}

            
            
			public Map<String, String> getArguments() {
				 Map<String, String> args = new LinkedHashMap<String, String>();
				 args.put("[source_file_name]","avro file location in hdfs");
				 args.put("[des_file_name]", "csv file location in local");
				 args.put("[threadCount]", "no of threads");
				 return args;
			}
		};
	}


	public Object execute(Context ctx) 
	{

		IOConsole console = ctx.getIoConsole();

		
		String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);
		
		
		
		// validating input

		if( args!=null && args.length==3 )
		{
		
			int threadCount = Integer.parseInt(args[2]);
			
			Main main = new Main();
			try 
			{
				long startTime = System.currentTimeMillis();
				
				main.bulkAvroToCSV(args[0], args[1],threadCount);
				
				long stopTime = System.currentTimeMillis();
				
				double d = (stopTime - startTime) / 1000;
				
				console.writeOutput("\n"+d + " Seconds taken.\n");
			} 
			catch (InterruptedException e)
			{
				console.writeOutput("\n" + e.getMessage() + "\n");
				
			} 
			catch (ExecutionException e) 
			{
				console.writeOutput("\n" + e.getMessage() + "\n");
				
			} catch (FileArgumentException e) {
				console.writeOutput("\n" + e.getMessage() + "\n");
			} catch (IOException e) {
				console.writeOutput("\n" + e.getMessage() + "\n");
			}
			
		}
		else
		{
			console.writeOutput(getDescriptor().getUsage());
			return ctx;
		    
		}
        

		
		return ctx;
	}

}
