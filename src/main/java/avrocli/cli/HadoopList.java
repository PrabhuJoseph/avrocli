package avrocli.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.AvroCliConstants;
import avrocli.avro.mapreduce.common.AvroCliHelper;

public class HadoopList implements Command {
	
    private static final String NAMESPACE = "syscmd";
    private static final String CMD_NAME = "ls";
    private static Configuration conf = new Configuration();
    private static FsShell fsShell = new FsShell(conf);

	public void plug(Context ctx) {
		
	}

	public Descriptor getDescriptor() {
		
		return new Command.Descriptor() {
			
			public String getUsage() {
				return "\nUSAGE:ls <[file: | hdfs:]directory_path>"+AvroCliConstants.NEWLINE;
			}
			
			public String getNamespace() {
				return NAMESPACE;
			}
			
			public String getName() {
				return CMD_NAME;
			}
			
			public String getDescription() {
				return "Lists the file under the given directory in HDFS";
			}
            
			public Map<String, String> getArguments() {
				 Map<String, String> args = new LinkedHashMap<String, String>();
				 args.put("directory_path", "Lists the files under the given directory in HDFS");
				 return args;
			}
		};
	}


	public Object execute(Context ctx) 
	{
		IOConsole console = ctx.getIoConsole();
		String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);
		
        if(args!=null && args.length == 1)
        {
        	try {
        		String[] fileParts = args[0].split(":");
        		if(fileParts[0].equals(AvroCliConstants.HDFS_FILESYSTEM)) {
        			ArrayList<String> listOfArgs = new ArrayList<String>();
        			listOfArgs.add(AvroCliConstants.HYPHEN+CMD_NAME);
        			listOfArgs.add(fileParts[1]);
        			ToolRunner.run(fsShell, listOfArgs.toArray(new String[listOfArgs.size()]));
        		} else if(fileParts[0].equals(AvroCliConstants.LOCAL_FILESYSTEM)) {
        			List<String> listOfFile = AvroCliHelper.getFileListFromLocal(fileParts[1], "*");
        			for(String file : listOfFile)
        				console.writeOutput(file+AvroCliConstants.NEWLINE);
        		} else {
        			console.writeOutput(getDescriptor().getUsage());
        		}
        	} catch(FileNotFoundException e) {
        		console.writeOutput("\n"+e.getMessage()+"\n");
        	} catch (IOException e) {
        		console.writeOutput("\n"+e.getMessage()+"\n");
			} catch(Exception e) {
        		e.printStackTrace();
        	}
          } else {
        	 console.writeOutput(getDescriptor().getUsage());
          }
		return ctx;
	}

}
