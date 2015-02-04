package avrocli.avro.mapreduce.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.clamshellcli.api.IOConsole;

import avrocli.avro.AvroCliConstants;
import avrocli.avro.ColumnNotFoundException;
import avrocli.avro.Deserialiser;
import avrocli.avro.FileArgumentException;

public class AvroCliHelper {
	
	private static FileSystem fs;
	private static Configuration conf = new Configuration();

	/*
	 * returns list of file given a directory and filePattern to match, give * for all files
	 */
	
	private static ArrayList<String> getFileListFromHDFS(String dirName,String pattern) throws IOException {
		ArrayList<String> listDir = new ArrayList<String>();
		FileStatus[] hdfsFileSystemFiles;
		fs = FileSystem.get(conf);
			hdfsFileSystemFiles = fs.listStatus((new Path(dirName)));
			if(hdfsFileSystemFiles.length!=0) {
				for (FileStatus file : hdfsFileSystemFiles) {
					if (file.isFile() && !(file.getPath().toString().contains("_SUCCESS"))) {
						if(pattern.equals("*")){
							listDir.add(file.getPath().toString());
						}
						else if(file.getPath().toString().contains(pattern)){
							listDir.add(file.getPath().toString());
						}
					}
				}
			}
		return listDir;
	}
	
		
		public static ArrayList<String> getFileListFromLocal(String dirName,String pattern) throws FileNotFoundException {
			  ArrayList<String> fileList = new ArrayList<String>();
			  File folder = new File(dirName);
			  File[] listOfFiles = folder.listFiles(); 
			  if(listOfFiles == null) {
				  throw new FileNotFoundException();
			  }
			  for (int i = 0; i < listOfFiles.length; i++) {
			   if (listOfFiles[i].isFile() && !listOfFiles[i].isHidden()) {
				   if(pattern.equals("*")) {
					   fileList.add(listOfFiles[i].getAbsolutePath().toString());
					} else if(listOfFiles[i].getAbsolutePath().toString().contains(pattern)) {
						fileList.add(listOfFiles[i].getAbsolutePath().toString());
					}
			     }
			 }
			return fileList;
			}
			

	public static List<String> getFileListFromDir(String dirName,String pattern,String scheme) throws IOException, FileArgumentException{
		ArrayList<String> list = new ArrayList<String>();
		if(scheme.equals(AvroCliConstants.HDFS_FILESYSTEM)) {
			list = getFileListFromHDFS(dirName,pattern);
		} else if(scheme.equals(AvroCliConstants.LOCAL_FILESYSTEM)) {
			list = getFileListFromLocal(dirName, pattern);
		} else {
			throw new FileArgumentException(scheme+" :Invalid scheme");
		}
		return list;
	}
	
	public static void readOutputFile(IOConsole console,String operation) throws IOException, FileArgumentException {
			BufferedReader br;
			List<String> listOfFiles = getFileListFromDir(AvroCliConstants.OUTPUT_DIR , "*" , AvroCliConstants.HDFS_FILESYSTEM);
			for(String file : listOfFiles){
			br = new BufferedReader(new InputStreamReader(new java.util.zip.InflaterInputStream(fs.open(new Path(file))))); 
			String line = null;
			while ((line = br.readLine()) != null) {
				console.writeOutput(line+AvroCliConstants.NEWLINE);
			}
		}
	}
	
	public static void validateColumn(ArrayList<String> onColumns,String fileName) throws ColumnNotFoundException, IOException, FileArgumentException {
			ArrayList<String> listOfColumns = new ArrayList<String>();
			GenericRecord user = null;
			//constructing pattern to match
		    String filePattern = fileName.substring(fileName.lastIndexOf("/")+1,fileName.length());
		    if(StringUtils.countMatches(filePattern,"*") == 2){
				filePattern = filePattern.substring(filePattern.indexOf("*") + 1, filePattern.lastIndexOf("*"));
			} else {
				filePattern = filePattern.indexOf("*") == 0 ? "*" : filePattern.substring(0, filePattern.indexOf("*"));
			}
		    if(fileName.split(":")[0].equals(AvroCliConstants.LOCAL_FILESYSTEM)){
		    	throw new FileArgumentException("Mapreduce will not run with local file system as source");
		    }
		    List<String> listOfFiles = getFileListFromDir(fileName.substring(0, fileName.lastIndexOf("/")),filePattern,AvroCliConstants.HDFS_FILESYSTEM);
		    if(listOfFiles.size() == 0) {
		    	throw new FileArgumentException("The given pattern " + filePattern + " did not match any files");
		    }
		    String aFile = listOfFiles.get(0);
			DataFileReader<GenericRecord> dataFileReader = new Deserialiser(aFile.replaceFirst("//CRSCLUSTER","")).getDataFileReader();
			Schema schema=dataFileReader.getSchema();
			List<Schema.Field> fields=schema.getFields();
			//removing empty elements from the list
			onColumns.removeAll(Collections.singleton(""));
			if (dataFileReader.hasNext()) {
			   user = dataFileReader.next(user);
			   for(String onColumn: onColumns){
	        	for(int j=0;j<fields.size();j++) {
	        		if(onColumn.equals(fields.get(j).name())) {
	        			listOfColumns.add(fields.get(j).name());
	        			 break;
	        			}
	        		}
			   }
	        if(listOfColumns.size() != onColumns.size()) {
	        	onColumns.removeAll(listOfColumns);
				   for(String missingColumn : onColumns){
					   throw new ColumnNotFoundException(missingColumn+" ");
				   }
	        		
		     }
		}
	}
}
