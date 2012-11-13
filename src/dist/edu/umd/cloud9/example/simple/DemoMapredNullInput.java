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

package edu.umd.cloud9.example.simple;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;

public class DemoMapredNullInput {
  private static final Logger LOG = Logger.getLogger(DemoMapredNullInput.class);

  private DemoMapredNullInput() {}

  private static class MyMapper extends NullMapper {
    public void run(JobConf conf, Reporter reporter) throws IOException {
      LOG.info("Counting to 10:");
      for (int i = 0; i < 10; i++) {
        LOG.info(i + 1 + "...");
        try {
          Thread.sleep(10000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Runs the demo.
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(DemoMapredNullInput.class);
    conf.setJobName("DemoMapredNullInput");

    conf.setNumMapTasks(10);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(NullInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(MyMapper.class);

    JobClient.runJob(conf);
  }
}
