package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;

public class ReadSequenceFile {

	public static void main(String[] args) throws IOException {
		if (args.length > 0)
			readSequenceRecords(args[0]);
		else
			System.out.println("Usage: ReadSequenceFile filename");
	}
	
	public static int readSequenceRecords(String in) throws IOException {
		System.out.println("***** Reading Sequence Records from:\n"+in);
		Configuration config = new JobConf();
		FileSystem fileSys = FileSystem.get(config);
		Path p= new Path(in);
		if(fileSys.isDirectory(p))
				return readSequenceFilesInDir(p);
		else return readSequenceFile(p);
	}
	
	public static int readSequenceFile(String filename) throws IOException {
		return readSequenceFile(new Path(filename));
	}
	
	
	public static int readSequenceFile(Path filename) throws IOException {
		JobConf config = new JobConf();
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), filename , config);
		
		System.out.println("**Opened Sequence File");
		System.out.println("Reading Sequence Records from:\n"+filename);
		System.out.println("Key type: " + reader.getKeyClass().toString());
		System.out.println("Value type: " + reader.getValueClass().toString());
		System.out.println("==================");
		Writable key, value;
		int n=0;
		try {
			key = (Writable) reader.getKeyClass().newInstance();
			value = (Writable) reader.getValueClass().newInstance();
			
			while (reader.next(key, value)) {
				System.out.println("Key: " + key + "\nValue: " + value);
				System.out.println("------------");
				n++;
			}
			reader.close();
			System.out.println(n + " records read.");
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return n;
	}
	
	public static void readSequenceFilesInDir(String filename) throws IOException {
		readSequenceFilesInDir(new Path(filename));
	}
	
	public static int readSequenceFilesInDir(Path inPath){
		JobConf config = new JobConf();
		int n=0;
		try {
			FileSystem fileSys = FileSystem.get(config);
			Path[] files=fileSys.listPaths(inPath);
			for(int i=0; i<files.length && i<1; i++){
				n+=readSequenceFile(files[i]);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(n + " records read.");
		return n;
	}
	
}
