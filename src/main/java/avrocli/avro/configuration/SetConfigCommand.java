package avrocli.avro.configuration;

import java.util.Collections;
import java.util.Map;

import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.configuration.Configuration;

public class SetConfigCommand implements Command {

	
	 private static final String NAMESPACE = "syscmd";
	 private static final String CMD_NAME = "setconfig";
	 
	public void plug(Context arg0) {
	}

	@Override
	public Object execute(Context ctx) {
		IOConsole console = ctx.getIoConsole();

		String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);
	
		Configuration conf = Configuration.getInstance();
		try {
			String oldValue = null;
			String newValue = args[1];
			for (String propertyKey : conf.getKeysAsList()) {
				if (propertyKey.contains(args[0])) {
					oldValue = conf.getProperty(propertyKey);
					conf.setProperty(propertyKey, newValue);
				}
			}

			if (oldValue != null) {
				console.writeOutput("Old Value => "+oldValue+"\n");
				console.writeOutput("New Value => "+newValue+"\n");
			} else {
				console.writeOutput("Property not found!");
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			console.writeOutput("Wrong number of arguments.");
		}
		return ctx;
	}
	@Override
	public Descriptor getDescriptor() {
		return new Command.Descriptor() {
			
			public String getUsage() {
				return "\nUSAGE: setconfig <property_name> <property_value>\n";
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
			return Collections.emptyMap();
			}
		};
	}

	

}
