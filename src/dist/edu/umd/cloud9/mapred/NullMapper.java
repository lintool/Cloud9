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

	private JobConf mConf = null;

	public void configure(JobConf conf) {
		mConf = conf;
	}

	public void map(NullWritable key, NullWritable value,
			OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
			throws IOException {
		run(mConf);
	}

	public abstract void run(JobConf conf) throws IOException;
}
