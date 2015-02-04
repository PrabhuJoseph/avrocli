package avrocli.cli;

import org.clamshellcli.api.Context;
import org.clamshellcli.api.SplashScreen;
import java.io.OutputStream;
import java.io.PrintStream;


public class AvroSplashScreen implements SplashScreen
{
                                           
    
    public void render(Context ctx) {
        PrintStream out = new PrintStream ((OutputStream)ctx.getValue(Context.KEY_OUTPUT_STREAM));
        
    	StringBuilder screen = new StringBuilder();
        
    	screen.append("\u001B[35;1m");
        screen
        .append(String.format("%n%n"))
        .append("   #     #     #  ######    ####       #####  #      #\n")
        .append("  # #    #     #  #     #  #    #     #       #      #\n")
        .append(" #####    #   #   ######   #    #     #       #      #\n")
        .append("#     #    # #    #   #    #    #     #       #      #\n")
        .append("#     #     #     #    #    ####       #####  ###### #\n\n")  
        .append("A command-line tool for AVRO VIEWER").append("\n"); 
        
        screen.append("\u001B[37;1m");
        out.println(screen);
    }

    public void plug(Context plug) {
    }
    
}
