package avrocli.cli;


import org.clamshellcli.api.Context;
import org.clamshellcli.api.Prompt;

public class AvroPrompt implements Prompt
{
  private static final String PROMPT = "\u001B[36;1mAVROQL> \u001B[37;1m";

  public String getValue(Context ctx)
  {
    return PROMPT;
  }

  public void plug(Context plug)
  {
    // nothing to do
  }

}
