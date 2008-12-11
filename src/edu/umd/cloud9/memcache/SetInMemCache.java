package edu.umd.cloud9.memcache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;


public class SetInMemCache {

	/** 
	 *  The Mapper class which takes in key-value pairs from Sequence File given as an input and writes the data to Memcache servers.
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {

		// variables to hold key and value
		Long keyTemp = new Long(0);
		Object obj ;
		
		// variable to hold the connection to the memcache servers.
		MemcachedClient memcacheClient;
		
		// Method to set up memcache connection from client to all servers. The list of servers is obtained 
		// from the JobConf variable set up in the main.
		public void configure(JobConf conf) {
			try {
				memcacheClient = new MemcachedClient(AddrUtil.getAddresses(conf.get("ADDRESSES")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Mapper : Puts the key- value pair in Memcache. 
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {
	
			// Convert the value to obj because Memcache takes (String, Obj) as key value pair
			obj = value.toString();
			keyTemp = (Long)key.get();
			
			// Putting value in MemCache
			memcacheClient.set(keyTemp.toString(),60*60*20,obj);
			// to fulfill the mapper configuration
			output.collect(key, value);
		}	
	}


	/*
	 * Default Constructor for the class
	 */
	protected SetInMemCache() {
	}

	/**
	 * This method takes in a text file which contains list of Ip Address, one per line and forms a single String variable
	 * 
	 * @param inputFile
	 * @return List of IP Addresses as a single string with default port appended to each server ip address
	 */
	private static String getListOfIpAddresses(String inputFile){
		String ipAddresses="";
		// default port
		String port="11211";
		try{
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
			String line;
			while((line = in.readLine())!=null){
				if(!line.equals("")){
					String temp = line+":"+port;
					if(ipAddresses.equals(""))
						ipAddresses = temp;
					else
						ipAddresses += " " + temp;
				}
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return ipAddresses;
	}
	/**
	 * Runs in
	 */
	public static void main(String[] args) throws IOException {

		/* 
		 * First argument - path of file on local file system on master node containing list of memcache servers
		 * Second argument - path of file on dfs on master node to be converted into sequence file and put in memcache 
		 */

		if(args.length != 2){
			System.out.println(" usage : [path of ip address file] [path of sequence file on dfs ]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];
		String inputPathSeqFile = args[1];
		
		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if(ipAddress.equals("")){
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		}else{
			System.out.println("List of IP addresses : "+ ipAddress);
		}
		
		
		String extraPath = "/shared/extraInfo";
		MemcachedClient myMCC;
		myMCC = new MemcachedClient(AddrUtil.getAddresses(ipAddress));
		myMCC.flush();
		int mapTasks = 1;
		int reduceTasks = 0;

		JobConf conf = new JobConf(SetInMemCache.class);
		conf.setJobName("SetInMemCache");

		conf.set("ADDRESSES", ipAddress);
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPathSeqFile));
		conf.setInputFormat(SequenceFileInputFormat.class);

		
		FileOutputFormat.setOutputPath(conf, new Path(extraPath));
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		
		
		System.out.println("getting: " + conf.get("ADDRESSES"));
		JobClient.runJob(conf);
	}
}
