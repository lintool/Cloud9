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

/**
 * This is a general file to read key - value pair from Memcache servers.
 * They program is a map-reduce cycle where they keys are passed into the Mapper through the general framework
 * The value for these keys are fetched from Memcache 
 * The map-reduce framework is used to get task done in parallel
 * @author Anand Bahety
 */
public class GetFromMemCache {
	/*
	 * This is used to add up total time for access to HDFS in map cycle	 
	 */
	static enum MyCounters {
		TIME;
	};
	
	/** 
	 *  The Mapper class which takes in key-value pairs from Sequence File given as an input and gets the data from Memcache servers.
	 */
	public static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {
		
		Long keyTemp = new Long(0);
	    MemcachedClient memcachedClient;
	    Object obj;
	    
	    // Method to set up memcache connection from client to all servers. The list of servers is obtained 
		// from the JobConf variable set up in the main.
	    public void configure(JobConf conf) {
			try {
				memcachedClient = new MemcachedClient(AddrUtil.getAddresses(conf.get("ADDRESSES")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	
		
		
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {


			// writing key value pair to cache			
		    keyTemp =(Long) key.get();
		    // start timer
		    long startTime = System.currentTimeMillis();
			obj = memcachedClient.get(keyTemp.toString());
			long endTime = System.currentTimeMillis();
			long diff = (endTime-startTime);
			// incrementing the timer to add the access time for the key from memcache
			reporter.incrCounter(MyCounters.TIME, diff);
			if ( obj == null)
				throw new RuntimeException("Error getting from memcache");
			
			output.collect(key, value);
		}	
	}

	protected GetFromMemCache() {
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

	/* 
	 * First argument - path of file on local file system on master node containing list of memcache servers
     * Second Argument - path of sequence file on hdfs
     */
	public static void main(String[] args) throws IOException {		

		if(args.length != 2){
			System.out.println(" usage : [path of ip address file] [path of sequence file on hdfs]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];
		String pathOfSeqFile = args[1];
		
		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if(ipAddress.equals("")){
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		}else{
			System.out.println("List of IP addresses : "+ ipAddress);
		}
		// out put directory for reducer
		String extraPath = "/shared/extraInfo";

		int mapTasks = 5;
		int reduceTasks = 0;

		JobConf conf = new JobConf(GetFromMemCache.class);
		conf.setJobName("GetFromMemCache");
		// setting the variable to hold ip addresses so that it can be available in the mapper
		conf.set("ADDRESSES", ipAddress);
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(pathOfSeqFile));
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		
		FileOutputFormat.setOutputPath(conf, new Path(extraPath));
	
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		// delete the directory if it already exists
		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);

		
		JobClient.runJob(conf);
		
	}
}
