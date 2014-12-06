/*
 * Cloud9: A Hadoop toolkit for working with big data
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

package edu.umd.cloud9.example.pagerank;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Ranger partitioner. In the context of graph algorithms, ensures that consecutive node ids are
 * blocked together.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RangePartitioner extends Partitioner<IntWritable, Writable> implements Configurable {
  private int nodeCnt = 0;
  private Configuration conf;

  public RangePartitioner() {}

  @Override
  public int getPartition(IntWritable key, Writable value, int numReduceTasks) {
    return (int) (((float) key.get() / (float) nodeCnt) * numReduceTasks) % numReduceTasks;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    configure();
  }

  private void configure() {
    nodeCnt = conf.getInt("NodeCount", 0);
  }
}
