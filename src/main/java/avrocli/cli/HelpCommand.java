package avrocli.cli;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.AvroCliConstants;


public class HelpCommand implements Command
{
  public static final String CMD_NAME = "help";
  public static final String NAMESPACE = "syscmd";
    
  private Command.Descriptor descriptor;

  public Object execute(Context ctx) throws IllegalArgumentException
  {
    String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);

    if(args != null && args.length == 1){
      printCommandHelp(ctx, args[0]);
    }else  {
    	printAllHelp(ctx);
    }
    ctx.getIoConsole().writeOutput(String.format("%n%n", new Object[0]));
	return null;
  }

  @Override
  public Descriptor getDescriptor()
  {
    return (descriptor != null) ? descriptor : (descriptor = new Command.Descriptor()
    {

      public String getNamespace()
      {
        return NAMESPACE;
      }

      public String getName()
      {
        return CMD_NAME;
      }

      public String getDescription()
      {
        return "Displays help information for available commands.";
      }

      public String getUsage()
      {
        return "Type 'help' or 'help [command_name]'";
      }

      public Map<String, String> getArguments()
      {
        return Collections.emptyMap();
      }
    });
  }

  private void printCommandHelp(Context ctx, String cmdName)
  {
    Map commands = ctx.mapCommands(ctx.getCommands());
    if (commands != null)
    {
      Command cmd = (Command) commands.get(cmdName.trim());
      if (cmd != null)
      {
        printCommandHelp(ctx, cmd);
      }
      else
      {
        ctx.getIoConsole().writeOutput(String.format("%nHelp for Command [%s] cannot not found.", new Object[] { cmdName }));
      }
    }
  }

  private void printCommandHelp(Context ctx, Command cmd)
  {
    if (cmd != null && cmd.getDescriptor() != null)
    {
      ctx.getIoConsole().writeOutput(String.format("%nCommand: %s - %s%n", new Object[] { cmd.getDescriptor().getName(), cmd.getDescriptor().getDescription() }));
      ctx.getIoConsole().writeOutput(String.format("Usage: %s", new Object[] { cmd.getDescriptor().getUsage() }));
      printCommandParamsDetail(ctx, cmd);
    }
    else
    {
      ctx.getIoConsole().writeOutput(String.format("%nUnable to display help for command.", new Object[0]));
    }
  }

  private void printCommandParamsDetail(Context ctx, Command cmd)
  {
    org.clamshellcli.api.Command.Descriptor desc = cmd.getDescriptor();
    if (desc == null || desc.getArguments() == null)
    {
      return;
    }
    IOConsole c = ctx.getIoConsole();
    c.writeOutput(String.format("%n%nOptions:", new Object[0]));
    c.writeOutput(String.format("%n--------", new Object[0]));
    java.util.Map.Entry entry;
    for (Iterator i$ = desc.getArguments().entrySet().iterator(); i$.hasNext(); c.writeOutput(String.format("%n%1$53s\t%2$s", new Object[] { entry.getKey(), entry.getValue() })))
    {
      entry = (java.util.Map.Entry) i$.next();
    }

  }

  private void printAllHelp(Context ctx)
  {
    IOConsole c = ctx.getIoConsole();
    c.writeOutput(String.format("%nAvailable Commands", new Object[0]));
    c.writeOutput(String.format("%n--------------------\n", new Object[0]));
    
    for (String commandNameSpace : AvroCliConstants.COMMAND_NAMESPACE) {
        c.writeOutput(String.format("%n%s", new Object[]{commandNameSpace}));
        c.writeOutput(String.format("%n------------------------%n", new Object[0]));

        List<Command> commands = ctx.getCommandsByNamespace(commandNameSpace);
        
        Command cmd;
        for (Iterator i$ = commands.iterator(); i$.hasNext(); c.writeOutput(String.format("%n%30s\t%s", new Object[] { cmd.getDescriptor().getName(), cmd.getDescriptor().getDescription() })))
        {
          cmd = (Command) i$.next();
        }
        c.writeOutput(String.format("%n", new Object[0]));
	}


  }

public void plug(Context arg0) {
	// TODO Auto-generated method stub
	
}
}
