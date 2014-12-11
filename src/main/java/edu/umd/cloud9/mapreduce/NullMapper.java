package edu.umd.cloud9.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public abstract class NullMapper extends
    Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
  private static final Logger LOG = Logger.getLogger(NullMapper.class);

  private static enum HEARTBEAT {
    BEAT
  };

  private static class HeartbeatThread extends Thread {
    private final Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context;

    public HeartbeatThread(
        Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context) {
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
  public void run(Mapper<NullWritable, NullWritable, NullWritable, NullWritable>.Context context) {
    Thread t = new HeartbeatThread(context);
    t.start();
    try {
      runSafely(context);
    } catch (Exception e) {
      t.interrupt();
      throw new RuntimeException(e);
    }

    // Once we return from method, kill the heartbeat thread.
    t.interrupt();
  }

  public abstract void runSafely(Context context);
}