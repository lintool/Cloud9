package edu.umd.cloud9.example.hits;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

/**
 * Range partitioner. In the context of graph algorithms, ensures that
 * consecutive node ids are blocked together.
 * 
 * @author Jimmy Lin
 * 
 */

public class RangePartitioner<K, V> implements
		Partitioner<IntWritable, Writable> {
	private int mNodeCnt = 0;

	public RangePartitioner() {
	}

	public int getPartition(IntWritable key, Writable value, int numReduceTasks) {
		return (int) (((float) key.get() / (float) mNodeCnt) * numReduceTasks) % numReduceTasks;
	}

	public void configure(JobConf job) {
		mNodeCnt = job.getInt("NodeCount", 0);
	}
}
