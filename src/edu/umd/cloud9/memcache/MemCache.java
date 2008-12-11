/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.memcache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.hadoop.io.Text;


public class MemCache {

	protected static MemcachedClient myMCC;
	final static int maxRead = 1000;

	private MemCache() {
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
	
	public static void initMemCache (String ip) throws IOException{
		myMCC = new MemcachedClient(AddrUtil.getAddresses(ip));
	}
	

	public static long putDataInMemcache() throws IOException{
		long count = 0;
		try{
		String inputPath = "/tmp/html.lst";
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));
		
		// initializing variables for writing in cache
		String line = "";
		Long cnt = new Long(0);
		Text txt = new Text("");
		
		while((line = reader.readLine())!=null){
			//reading values for cache
			
			cnt = count;
			Object value = line.toString();
			
			// writing key value pair to cache			
			Future<Boolean> obj = myMCC.set(cnt.toString(),60*60*24,value);
			// incrementing the counter			
			count++;
			
			
			// if not written, then we get queue full exception because queue gets full and operations are still waiting
			if(count%1000==0)
				while(!myMCC.waitForQueues(4, java.util.concurrent.TimeUnit.SECONDS));
			
		}	
		reader.close();
		
		System.out.println("\n number of records added to the cache are" + count);
		
		}catch (Exception e){
			e.printStackTrace();
		}
		
		return count;
	}
	
	public static void readDataFromMemcache(long count) throws IOException{
		
		Random r  = new Random();
		Long key = new Long(0);
		Object  value = new Object();
		long startTime = System.currentTimeMillis();
		
		long countOfRead = 0;
		while(countOfRead < 156215){
			key = countOfRead;
		//	key = ((Double)(r.nextDouble()*count)).longValue();
			value = myMCC.get(key.toString());	
			if(value==null)
				throw new RuntimeException("null value encountered for "+ key);
			//System.out.println(" value of key is " + key + " = " + value);
			countOfRead++;
		}
		
		long endTime = System.currentTimeMillis();
		long diff = ((endTime-startTime));///1000.0f);
		System.out.println("\n Starttime " + startTime + " end time = " + endTime);
		System.out.println("\n Total time taken is : " + (endTime-startTime));
				
	}
	
	public static void readDataFromMemcache(String inputPath) throws IOException{
		Object obj;
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));

		long startTime = System.currentTimeMillis();
		String line;
		long count=0;
		while((line=reader.readLine())!=null){
			StringTokenizer token = new StringTokenizer(line);
			while(token.hasMoreTokens()){
				String word = token.nextToken();
				obj = myMCC.get(word);	
				if(obj==null){
					System.out.println("Value not found "+ word);
					System.exit(1);
				}
			}
		}			
		long endTime = System.currentTimeMillis();
		long diff = ((endTime-startTime));///1000.0f);
		System.out.println("\n Starttime " + startTime + " end time = " + endTime);
		System.out.println("\n Total time taken is : " + (endTime-startTime));
				
	}
	
	
	public static void main(String[] args) throws IOException, ExecutionException,
			InterruptedException {
		/* 
		 * First argument - path of file on local file system on master node containing list of memcache servers
		 */

		if(args.length != 2){
			System.out.println(" usage : [path of ip address file] [path of text file] ");
			System.exit(1);
		}
		String pathOfIpAddressFile = args[0];
		String ipAddress = getListOfIpAddresses(pathOfIpAddressFile);
		if(ipAddress.equals("")){
			System.out.println("List of Memcache servers IP Addresses not available");
			System.exit(1);
		}else{
			System.out.println("List of IP addresses : "+ ipAddress);
		}
		
		
		MemCache.initMemCache(ipAddress);
		//long count = WriteMemCache1.putDataInMemcache();
		MemCache.readDataFromMemcache(args[1]);

	}
	
	


}
