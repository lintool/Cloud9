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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapreduce.NullInputFormat;
import edu.umd.cloud9.mapreduce.NullMapper;

public class DemoMapreduceNullInput {
  private static final Logger LOG = Logger.getLogger(DemoMapreduceNullInput.class);

  private DemoMapreduceNullInput() {}

  private static class MyMapper extends NullMapper {
    @Override
    public void runSafely(Context context) {
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
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(new Configuration());
    job.setJobName(DemoMapreduceNullInput.class.getSimpleName());
    job.setJarByClass(DemoMapreduceNullInput.class);

    job.setNumReduceTasks(0);
    job.setInputFormatClass(NullInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(MyMapper.class);

    job.waitForCompletion(true);
  }
}
