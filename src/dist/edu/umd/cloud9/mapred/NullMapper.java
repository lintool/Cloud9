package edu.umd.cloud9.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public abstract class NullMapper extends MapReduceBase implements
		Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

	static enum Heartbeat {
		COUNT
	};

	private JobConf mConf = null;

	// The sole job of this thread is to increment counters once in a while
	// to let the job track know we're still alive.
	private static class HeartbeatRunnable implements Runnable {
		Reporter mReporter;

		public HeartbeatRunnable(Reporter reporter) {
			mReporter = reporter;
		}

		public void run() {
			while (true) {
				try {
					mReporter.incrCounter(Heartbeat.COUNT, 1);
					Thread.sleep(60000);
				} catch (InterruptedException e) {
          break;
				}
			}
		}
	}

	public void configure(JobConf conf) {
		mConf = conf;
	}

	public void map(NullWritable key, NullWritable value,
			OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
			throws IOException {

		Thread pulse = new Thread(new HeartbeatRunnable(reporter));
		pulse.start();

		run(mConf, reporter);

		// Once we return from method, kill the heartbeat thread.
		pulse.interrupt();
	}

	public abstract void run(JobConf conf, Reporter reporter) throws IOException;
}
