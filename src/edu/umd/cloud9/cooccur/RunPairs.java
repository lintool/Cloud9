package edu.umd.cloud9.cooccur;

import org.apache.hadoop.conf.Configuration;

public class RunPairs {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();

		config.set("CollectionName", "sample");
		config.setInt("NumMapTasks", 10);
		config.setInt("NumReduceTasks", 10);
		config.set("InputPath", "/shared/sample-input/bible+shakes.nopunc");
		config.set("OutputPath", "CooccurrenceMatrixPairs");
		config.setInt("Window", 2);

		ComputeCooccurrenceMatrixPairs task = new ComputeCooccurrenceMatrixPairs(config);
		task.run();
	}
}
