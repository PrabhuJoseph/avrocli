package avrocli.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import jline.ConsoleReader;

import org.clamshellcli.api.Command;
import org.clamshellcli.api.Context;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.ColumnNotFoundException;
import avrocli.avro.FileArgumentException;
import avrocli.avro.IllegalAggregateColumnException;
import avrocli.avro.Main;
import avrocli.avro.QueryMalformedException;
import avrocli.avro.mapreduce.common.AvroCliHelper;
import avrocli.avro.mapreduce.common.CommonDriver;

public class SelectCommand implements Command {
	private String csvName = System.getProperty("user.home") + "/users_"+getPID()+".csv";

	private static final String NAMESPACE = "syscmd";
	private static final String CMD_NAME = "select";
	public void plug(Context ctx) {

	}

        public static long getPID() {
		    String processName =
		      java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
		    return Long.parseLong(processName.split("@")[0]);
	}
	

	private void displayRecords(final IOConsole console) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(csvName));
			String line = null;

			//int pagination = 0;
			//String prompt = "";

			
			ConsoleReader consoleReader = new ConsoleReader();
			consoleReader.setUsePagination(true);
			int terminalWidth = consoleReader.getTermwidth();
			List<String> result = new ArrayList<String>();
			
			if((line = br.readLine()) != null)
			{
				
				result.add(String.format("%"+terminalWidth+"s", line));
				result.add(String.format("%"+terminalWidth+"s", "\n"));
				
				StringBuffer lineBreak=new StringBuffer();
				
				for(int i=0;i<terminalWidth;i++)
				{
					lineBreak.append("-");
				}
				result.add(String.format("%"+terminalWidth+"s", lineBreak));
				result.add(String.format("%"+terminalWidth+"s", "\n"));
			}

			while ((line = br.readLine()) != null) {

				/*
				 * if(pagination==5) {
				 * console.writeOutput("\nPress Enter for more data...");
				 * pagination=0; console.readInput(prompt); }
				 * console.writeOutput(line+"\n");
				 * 
				 * pagination++;
				 * 
				 * }
				 */
				// console.writeOutput("\n");
				result.add(String.format("%"+terminalWidth+"s", line));
				
			}
			
			result.add(String.format("%"+terminalWidth+"s", "\n"));
			consoleReader.printColumns(result);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
                  finally{
			File file = new File(csvName);
			file.delete();
	        }

	}

	public Object execute(Context ctx) {
		IOConsole console = ctx.getIoConsole();

		String[] args = (String[]) ctx.getValue(Context.KEY_COMMAND_LINE_ARGS);
		
		// ROUGH QUERY PARSER

		// Mandatory select arguments
		if (args != null && args.length >= 3 && args[1].trim().equalsIgnoreCase("from")) 
		{
			
			String columns[] = args[0].split(",");
			String fileName = args[2].trim();
			
			boolean isMapReduceOn = fileName.contains("*") ? true : false;
			boolean displayFlag = true;
			String groupbyColumn = "";
                        StringBuilder sqlStmt = new StringBuilder();
                        NumberFormat formatter = new DecimalFormat("#0.0000");     

			
            //where clause
            if (args.length > 3) {

					if (((args[3].equalsIgnoreCase("where") || args[3].equalsIgnoreCase("into")) && args.length > 4) || args[3].contains("groupby") ) {

						for (int i = 3; i < args.length; i++) {
							if (args[i].equalsIgnoreCase("into")) {
								if (i == args.length - 2) {
									displayFlag = false;
									csvName = args[i + 1];
									break;
								} else {
									 console.writeOutput(getDescriptor().getUsage());
									 return ctx;
								}

							} else {
								sqlStmt.append(args[i] + " ");
							}
						}
					} else {
						console.writeOutput(getDescriptor().getUsage());
						return ctx;
					}

				}

				if (sqlStmt.toString().trim().equalsIgnoreCase("where")) {
					console.writeOutput(getDescriptor().getUsage());
					return ctx;
				}

				Main main = new Main();
				

				try {
					String groupBy = args[args.length-1];
					if(groupBy.startsWith("groupby(") && groupBy.endsWith(")")) {
						groupbyColumn = groupBy.substring(8,groupBy.length()-1);
						sqlStmt = new StringBuilder(sqlStmt.substring(0, sqlStmt.lastIndexOf("groupby")).trim());
					} 
					long startTime = System.currentTimeMillis();
					long count = 0;

					if (columns[0].equalsIgnoreCase("count(*)")) {
						if (columns.length == 1) {
							
							if(isMapReduceOn){
									StringBuilder argsToMapReduce = new StringBuilder();
									CommonDriver.trigger("count",fileName,sqlStmt.toString(),argsToMapReduce.toString(),groupbyColumn);
									AvroCliHelper.readOutputFile(console,"count");
							} else {
							count = main.count(sqlStmt.toString().trim(),
									fileName);
							
							long stopTime = System.currentTimeMillis();

							double d = (stopTime - startTime) / 1000;

							console.writeOutput(count + " Rows present. " + d+ " Seconds taken.\n");
							
						} 
						}else {
							console.writeOutput(getDescriptor().getUsage());
							return ctx;

						}

					} 
					else if (columns[0].startsWith("sum(") && columns[0].endsWith(")")) {
							if (columns.length == 1) {
								double sum = 0.0d;
								String sumColumn = columns[0].substring(4,columns[0].length()-1);
								if(isMapReduceOn) {
									ArrayList<String> columnsToValidate = new ArrayList<String>();
									columnsToValidate.add(sumColumn);
									columnsToValidate.add(groupbyColumn);
									AvroCliHelper.validateColumn(columnsToValidate,fileName);
									StringBuilder argsToMapReduce = new StringBuilder();
									argsToMapReduce.append(sumColumn);
									CommonDriver.trigger("sum",fileName,sqlStmt.toString(),argsToMapReduce.toString(),groupbyColumn);
									AvroCliHelper.readOutputFile(console,"sum");
								} else {
									sum = main.sum(sumColumn,sqlStmt.toString().trim(),
										fileName);
								
								long stopTime = System.currentTimeMillis();
								double d = (stopTime - startTime) / 1000;

								console.writeOutput("Sum of "+sumColumn+" = "+formatter.format(sum)+". "+d+ " Seconds taken.\n");
								}
							} else {
								console.writeOutput(getDescriptor().getUsage());
								return ctx;

							}

						}
					else if (columns[0].startsWith("min(") && columns[0].endsWith(")")) {
						double min=0.0d;
						if (columns.length == 1) {
							String minColumn = columns[0].substring(4,columns[0].length()-1);
							if(isMapReduceOn) {
								ArrayList<String> columnsToValidate = new ArrayList<String>();
								columnsToValidate.add(minColumn);
								columnsToValidate.add(groupbyColumn);
								AvroCliHelper.validateColumn(columnsToValidate,fileName);
								StringBuilder argsToMapReduce = new StringBuilder();
								argsToMapReduce.append(minColumn);
								CommonDriver.trigger("min",fileName,sqlStmt.toString(),argsToMapReduce.toString(),groupbyColumn);
								AvroCliHelper.readOutputFile(console,"min");
								} else {
							min = main.min(minColumn,sqlStmt.toString().trim(),
									fileName);
							
							long stopTime = System.currentTimeMillis();
							double d = (stopTime - startTime) / 1000;

							console.writeOutput("Min of "+minColumn+" = "+formatter.format(min)+". "+d+ " Seconds taken.\n");
								}
						} else {
							console.writeOutput(getDescriptor().getUsage());
							return ctx;

						}

					}
					else if (columns[0].startsWith("max(") && columns[0].endsWith(")")) {
						double max=0.0d;
						if (columns.length == 1) {
							String maxColumn = columns[0].substring(4,columns[0].length()-1);
							if(isMapReduceOn) {
								ArrayList<String> columnsToValidate = new ArrayList<String>();
								columnsToValidate.add(maxColumn);
								columnsToValidate.add(groupbyColumn);
								AvroCliHelper.validateColumn(columnsToValidate,fileName);
								StringBuilder argsToMapReduce = new StringBuilder();
								argsToMapReduce.append(maxColumn);
								CommonDriver.trigger("max",fileName,sqlStmt.toString(),argsToMapReduce.toString(),groupbyColumn);
								AvroCliHelper.readOutputFile(console,"max");
								} else {
							max = main.max(maxColumn,sqlStmt.toString().trim(),
									fileName);
							
							long stopTime = System.currentTimeMillis();
							double d = (stopTime - startTime) / 1000;

							console.writeOutput("Max of "+maxColumn+" = "+formatter.format(max)+". "+d+ " Seconds taken.\n");
							}
						} else {
							console.writeOutput(getDescriptor().getUsage());
							return ctx;

						}

					} else if(columns[0].startsWith("distinct(") && columns[0].endsWith(")")){
						if (columns.length == 1) {
							String distinctColumn = columns[0].substring(9,columns[0].length()-1);
							if(isMapReduceOn) {
								ArrayList<String> columnsToValidate = new ArrayList<String>();
								columnsToValidate.add(distinctColumn);
								AvroCliHelper.validateColumn(columnsToValidate,fileName);
								StringBuilder argsToMapReduce = new StringBuilder();
								argsToMapReduce.append(distinctColumn);
								CommonDriver.trigger("distinct",fileName,sqlStmt.toString(),argsToMapReduce.toString(),groupbyColumn);
								AvroCliHelper.readOutputFile(console,"distinct");
								} else {
									console.writeOutput("distinct() applicable only in mapreduce for now. Give * as pattern to run as mapreduce job"+"\n");
								}
						} else {
							console.writeOutput(getDescriptor().getUsage());
							return ctx;
						}
					}
					else {
						count = main.select(columns, sqlStmt.toString().trim(),
								fileName, csvName);

						if (count != -1) {
							if (displayFlag) {
								displayRecords(console);
							}
						} else {
						      console.writeOutput("AVRO READER is hanging. TRY AGAIN \n");
							  return ctx;
						}
						

						long stopTime = System.currentTimeMillis();
						double d = (stopTime - startTime) / 1000;

						console.writeOutput(count + " Rows present. " + d+ " Seconds taken.\n");
					}

				} catch (ExecutionException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (InterruptedException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (IOException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (QueryMalformedException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (ColumnNotFoundException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (FileArgumentException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (IllegalAggregateColumnException e) {
					console.writeOutput("\n" + e.getMessage() + "\n");
				} catch (Exception e) {
					e.printStackTrace();
				}
				

		} else {
			console.writeOutput(getDescriptor().getUsage());
		}

		return ctx;
	}


	
	
	public Descriptor getDescriptor() {

		return new Command.Descriptor() {

			public String getUsage() {
				return "\nUSAGE: select <operation_on_column> from <hdfs:/file: Avro file Name> where <Column Name>=<value> and/or .. groupby<column_name>\n";
			}

			public String getNamespace() {
				return NAMESPACE;
			}

			public String getName() {
				return CMD_NAME;
			}

			public String getDescription() {
				return "select the records from given AVRO file.";
			}

			public Map<String, String> getArguments() {
				Map<String, String> args = new LinkedHashMap<String, String>();
				args.put("[sum(<column_name>]","gives sum of the given column");
				args.put("[min(<column_name>]","gives min of the given column");
				args.put("[max(<column_name>]","gives max of the given column");
				args.put("[distinct(<column_name>]","gives distinct of the given column");
				args.put("[groupby<column_name>]", "gives output based on the groupby column");
				return args;
			}
		};

	}

}
