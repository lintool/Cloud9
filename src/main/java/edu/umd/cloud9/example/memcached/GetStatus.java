package edu.umd.cloud9.example.memcached;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketAddress;
import java.util.Map;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

public class GetStatus {

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

		if (args.length != 1) {
			System.out
					.println(" usage : [path of ip address file] [path of sequence file on dfs ]");
			System.exit(1);
		}

		String pathOfIpAddressFile = args[0];

		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if (ipAddress.equals("")) {
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		} else {
			System.out.println("List of IP addresses : " + ipAddress);
		}

		long total_items = 0;
		// Flush the memcache servers before setting the values
		MemcachedClient myMCC;
		myMCC = new MemcachedClient(AddrUtil.getAddresses(ipAddress));

		Map<SocketAddress, Map<String, String>> stats = myMCC.getStats();
		
		for ( Map.Entry<SocketAddress, Map<String, String>> e : stats.entrySet()) {
			System.out.println("memcached server: " + e.getKey().toString());
			
			for (Map.Entry<String, String> s : e.getValue().entrySet()) {
				System.out.println(" - " + s.getKey() + ": " + s.getValue());
				
				if ( s.getKey().equals("curr_items"))
					total_items += Long.parseLong(s.getValue());
				
			}
		}
		
		
		System.out.println("Total number items in memcache: " + total_items);
	}
}
