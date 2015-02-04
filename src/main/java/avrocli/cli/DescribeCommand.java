package avrocli.cli;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.FileArgumentException;
import avrocli.avro.Main;

public class DescribeCommand implements Command 

{
	
    private static final String NAMESPACE = "syscmd";
    private static final String CMD_NAME = "describe";
 

	public void plug(Context ctx) {
		
	}


	public Descriptor getDescriptor() {
		
		return new Command.Descriptor() {
			
			public String getUsage() {
				return "\nUSAGE: describe <hdfs:/file: Avro file Name>\n";
			}
			
			public String getNamespace() {
				return NAMESPACE;
			}
			
			public String getName() {
				return CMD_NAME;
			}
			
			public String getDescription() {
				return "Describe the schema for given AVRO file.";
			}

            
            
			public Map<String, String> getArguments() {
				 Map<String, String> args = new LinkedHashMap<String, String>();
				 args.put("[filename]", "filename with hdfs: or file: as scheme");
				 return args;
			}
		};
	}


	public Object execute(Context ctx) 
	{

		IOConsole console = ctx.getIoConsole();

		
		String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);
		
		// validating input
	
        if(args!=null && args.length == 1)
        {
        	String fileName = args[0].trim();
        	
        	Main main=new Main();
        	
        	try {
        		
				Map<String, String> schema = main.describe(fileName);
			
				
				int keylen=0,valuelen=0;
				
				for(Map.Entry<String, String> rows : schema.entrySet())
					{
					keylen = keylen<rows.getKey().length() ? rows.getKey().length() : keylen;
					valuelen =  valuelen<rows.getValue().length() ? rows.getValue().length() : valuelen;
					}
		
				console.writeOutput("\n");
				console.writeOutput(String.format("%-"+(keylen)+"s", "NAME")+"\t\t"+"DATA TYPE"+"\n");
				console.writeOutput("\n");
				for(int i=0;i<keylen;i++)  
				{
					console.writeOutput("-");
				}
				console.writeOutput("\t\t");
				for(int i=0;i<valuelen;i++) 
				{
					console.writeOutput("-");
				}
				console.writeOutput("\n");
				for(Map.Entry<String, String> rows : schema.entrySet())	
					console.writeOutput(String.format("%-"+(keylen)+"s", rows.getKey())+"\t\t"+rows.getValue()+"\n");
				
				
				console.writeOutput("\n");
				
			} 
        	catch (IOException e)
        	{
        		console.writeOutput("\n"+e.getMessage()+"\n");
			}
        	catch (InterruptedException e)
        	{
        		console.writeOutput("\n"+e.getMessage()+"\n");
			} 
        	catch (FileArgumentException e) 
			{
				console.writeOutput("\n"+e.getMessage()+"\n");
			}
          }
          else
          {
        	 console.writeOutput(getDescriptor().getUsage());
          }

		
		return ctx;
	}

}
