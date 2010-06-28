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

package edu.umd.cloud9.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Ranger partitioner. In the context of graph algorithms, ensures that
 * consecutive node ids are blocked together.
 * 
 * @author jimmy
 * 
 */
public class RangePartitioner<K, V> implements Partitioner<IntWritable, Writable> {
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
