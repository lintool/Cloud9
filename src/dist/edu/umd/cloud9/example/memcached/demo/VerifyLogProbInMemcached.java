package edu.umd.cloud9.example.memcached.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class VerifyLogProbInMemcached {

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

	public static void main(String[] args) throws IOException {

		/*
		 * 
		 */

		if (args.length != 2) {
			System.out
					.println(" usage : [path of ip address file] [path of sequence file on dfs ]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];
		String inputPathSeqFile = args[1];

		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		MemcachedClient myMCC = new MemcachedClient(AddrUtil.getAddresses(ipAddress));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(inputPathSeqFile),
				new Configuration());

		Text text = new Text();
		FloatWritable f = new FloatWritable();

		long startTime = System.currentTimeMillis();
		int cnt = 0;
		while (reader.next(text, f)) {
			if ( cnt % 1000 == 0 )
				System.out.print(".");
			
			// System.out.println(text + " " + f);
			Object obj = myMCC.get(text.toString());
			if (obj == null)
				throw new RuntimeException("Error getting from memcache: key=" + text);
			cnt++;
		}
		reader.close();
		long endTime = System.currentTimeMillis();
		long diff = (endTime-startTime);
		
		System.out.println("Verified " + cnt + " in " + diff + " ms.");
	}
}
