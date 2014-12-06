package edu.umd.cloud9.example.memcached.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class DemoMemcachedAccess {
	/*
	 * This is used to add up total time for access to HDFS in map cycle
	 */
	static enum MyCounters {
		TIME;
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, FloatWritable> {

		// Long keyTemp = new Long(0);
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

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			FloatWritable totalProb = new FloatWritable();
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			float sum = 0;
			while (itr.hasMoreTokens()) {
				String temp = itr.nextToken();

				// Ignore words that are too long...
				if (temp.toString().length() > 100)
					continue;

				// timer starts
				long startTime = System.currentTimeMillis();
				// access the memcached servers to get log prob of the word
				Object obj = memcachedClient.get(temp);
				// end timer
				long endTime = System.currentTimeMillis();
				long diff = (endTime - startTime);

				// incrementing the counter
				reporter.incrCounter(MyCounters.TIME, diff);
				if (obj == null)
					throw new RuntimeException("Error getting from memcache: key = " + temp);
				// adding the log prob
				sum = sum + Float.parseFloat(obj.toString());
			}
			totalProb.set(sum);
			output.collect(key, totalProb);

		}

		public void close() {
			memcachedClient.shutdown();
		}
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

	protected DemoMemcachedAccess() {
	}

	/**
	 * The main method takes three arguments from the command line 1. Path of
	 * the file containing ip addresses on local file system 2. Path of the
	 * sequence file on HDFS. This is the file which will be read by mappers and
	 * base on the words in the line read, there will be a probe to MemCache to
	 * find the log probability 3. Number of Map tast you want to generate in
	 * the map reduce cycle.
	 * 
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.out
					.println(" usage : [path of ip address file] [path of sequence file on hdfs] [no of Map Tasks]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];
		String inputPath = args[1];

		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if (ipAddress.equals("")) {
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		} else {
			System.out.println("List of IP addresses : " + ipAddress);
		}
		String extraPath = "/results";

		int mapTasks = Integer.parseInt(args[2]);
		// No need of reducer
		int reduceTasks = 0;

		JobConf conf = new JobConf(DemoMemcachedAccess.class);
		conf.setJobName("DemoMemcachedAccess");
		// setting the variable to hold ip addresses so that it can be available
		// in the mapper
		conf.set("ADDRESSES", ipAddress);
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(TextInputFormat.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(conf, outputDir);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		long endTime = System.currentTimeMillis();
		long diff = (endTime - startTime);

		System.out.println("Total job completion time (ms): " + diff);
	}
}
