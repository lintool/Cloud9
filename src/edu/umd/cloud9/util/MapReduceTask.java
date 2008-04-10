package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configuration;

public interface MapReduceTask {
	
	public void initialize(Configuration config);
	
	public void run(Configuration config) throws Exception;
	
	public void run() throws Exception;
}
