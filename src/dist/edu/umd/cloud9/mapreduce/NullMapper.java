package edu.umd.cloud9.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public abstract class NullMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
	private static final Logger LOG = Logger.getLogger(NullMapper.class);
	private static enum HEARTBEAT { BEAT };

	private static class ReducerHeartbeatThread extends Thread {
		private final Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context;

		public ReducerHeartbeatThread(Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context) {
			this.context = context;
		}

		@Override
		public void run() {
			while (true) {
				try {
					sleep(60000);
					LOG.info("Sending heartbeat...");
					context.getCounter(HEARTBEAT.BEAT).increment(1);
					context.setStatus("Sending heartbeat...");
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	@Override
	public void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
	      Thread t = new ReducerHeartbeatThread(context);
	      t.start();
	      run(context);
	      t.interrupt();
	}

	public abstract void run(Context context) throws IOException, InterruptedException;
}