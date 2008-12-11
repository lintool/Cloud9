package edu.umd.cloud9.memcache.WordLogProb;

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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import edu.umd.cloud9.io.Tuple;

public class GetLogProbFromMemCache {

	static enum MyCounters {
		TIME;
	};


	public static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, Tuple, Text, FloatWritable> {

		Long keyTemp = new Long(0);
		Object obj ;
		MemcachedClient m;

		public void configure(JobConf conf) {
			try {
				m = new MemcachedClient(AddrUtil.getAddresses(conf.get("ADDRESSES")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		
		public void map(LongWritable key, Tuple value, OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException {
			FloatWritable lw=new FloatWritable();
			String line = (String) value.get(0);
			StringTokenizer itr = new StringTokenizer(line);
			float sum=0;
			while (itr.hasMoreTokens()) {
				String temp=itr.nextToken();
				long startTime = System.currentTimeMillis();
				Object obj = m.get(temp);
				long endTime = System.currentTimeMillis();
				long diff = (endTime-startTime);
				reporter.incrCounter(MyCounters.TIME, diff);
				if ( obj == null)
					throw new RuntimeException("Error getting from memcache");
				sum=sum+Float.parseFloat(obj.toString());
			}
			lw.set(sum);
			output.collect(new Text(line), lw);

		}	
	}

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
	protected GetLogProbFromMemCache() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		
		if(args.length != 3){
			System.out.println(" usage : [path of ip address file] [path of sequence file on hdfs] [no of Map Tasks]");
			System.exit(1);
		}
		
		String pathOfIpAddressFile = args[0];
		String inputPath = args[1];
		
		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if(ipAddress.equals("")){
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		}else{
			System.out.println("List of IP addresses : "+ ipAddress);
		}
		String extraPath = "/shared/extraInfo"; 

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 0;

		JobConf conf = new JobConf(GetLogProbFromMemCache.class);
		conf.setJobName("GetFromMemCache");
		conf.set("ADDRESSES", ipAddress); 
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(conf, outputDir);
		
		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		long endTime = System.currentTimeMillis();
		float diff = (float)((endTime-startTime));///1000.0f);
		System.out.println("\n Starttime " + startTime + " end time = " + endTime);
		System.out.println("\n Total time taken is : " + diff);
	}
}
