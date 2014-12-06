package edu.umd.cloud9.example.memcached.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class SetLogProbInMemcached {

	private static class MyMapper extends MapReduceBase implements
			Mapper<Text, FloatWritable, Text, FloatWritable> {

		// Float keyTemp = new Float(0);
		// Object obj ;
		MemcachedClient memcachedClient;

		// Method to set up memcache connection from client to all servers. The
		// list of servers is obtained
		// from the JobConf variable set up in the main.
		public void configure(JobConf conf) {
			try {
				memcachedClient = new MemcachedClient(AddrUtil.getAddresses(conf.get("ADDRESSES")));
				Thread.sleep(500);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(Text text, FloatWritable value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

			// Ignore words that are too long...
			if (text.toString().length() > 100)
				return;

			// writing key value pair to cache
			Object obj = ((Float) (value.get())).toString();
			memcachedClient.set(text.toString(), 60 * 60 * 20, obj);

			try {
				Thread.sleep(1);
			} catch (Exception e) {
				e.printStackTrace();
			}

			// to fulfill the mapper configuration
			// output.collect(text, value);
		}

		public void close() {
			memcachedClient.shutdown();
		}
	}

	/*
	 * default constructor
	 */
	public SetLogProbInMemcached() {

	}

	/**
	 * This method takes in a text file which contains list of Ip Address, one
	 * per line and forms a single String variable
	 * 
	 * @param inputFile
	 * @return List of IP Addresses as a single string with default port
	 *         appended to each server ip address
	 */
	private static String getListOfIpAddresses(String inputFile) {
		String ipAddresses = "";
		// default port
		String port = "11211";
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(
					inputFile)));
			String line;
			while ((line = in.readLine()) != null) {
				if (!line.equals("")) {
					String temp = line + ":" + port;
					if (ipAddresses.equals(""))
						ipAddresses = temp;
					else
						ipAddresses += " " + temp;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return ipAddresses;
	}

	/**
	 * First argument - path of file on local file system on master node
	 * containing list of memcache servers Second argument - path of file on dfs
	 * on master node to be converted into sequence file and put in memcache
	 */
	public static void main(String[] args) throws IOException {

		/*
		 * 
		 */

		if (args.length != 3) {
			System.out
					.println(" usage : [path of ip address file] [path of sequence file on dfs ] [num mappers]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];
		String inputPathSeqFile = args[1];

		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if (ipAddress.equals("")) {
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		} else {
			System.out.println("List of IP addresses : " + ipAddress);
		}

		// Path for output of reducer.
		String extraPath = "/tmp";
		// Flush the memcache servers before setting the values
		MemcachedClient myMCC;
		myMCC = new MemcachedClient(AddrUtil.getAddresses(ipAddress));
		myMCC.flush();
		myMCC.shutdown();

		// Number of maptask has to be one else some values get converted to
		// null in memcache.
		// TODO : Check why this happens
		int mapTasks = Integer.parseInt(args[2]); // Integer.parseInt(args[2]);
		int reduceTasks = 0;

		JobConf conf = new JobConf(SetLogProbInMemcached.class);
		conf.setJobName("SetLogProbInMemcached");
		// setting the variable to hold ip addresses so that it can be available
		// in the mapper
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
