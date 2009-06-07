package edu.umd.cloud9.demo;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

public class DemoNullInput {

	private static final Logger sLogger = Logger.getLogger(DemoNullInput.class);

	private static class MyMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			sLogger.info("Counting to 10:");
			for (int i = 0; i < 10; i++) {
				sLogger.info(i + 1 + "...");
				try {
					Thread.sleep(10000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	protected DemoNullInput() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(DemoNullInput.class);
		conf.setJobName("DemoNullInput");

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(MyMapper.class);

		JobClient.runJob(conf);
	}
}
