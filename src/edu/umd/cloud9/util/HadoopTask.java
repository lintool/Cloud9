package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public abstract class HadoopTask implements Configurable {

	private Configuration mConf;

	public HadoopTask(Configuration conf) {
		mConf = conf;

		if (getRequiredParameters() == null)
			throw new RuntimeException("Error: getRequiredParameters() returns null!");
	}

	public void checkRequiredParametersPresent() {
		if (getRequiredParameters() == null)
			throw new RuntimeException("Error: getRequiredParameters() returns null!");

		for (String param : getRequiredParameters()) {
			if (mConf.get(param) == null) {
				throw new RuntimeException("Error: required parameter '" + param + "' not defined");
			}
		}
	}

	public Configuration getConf() {
		return mConf;
	}

	public void setConf(Configuration conf) {
		mConf = conf;
	}

	public void run() throws Exception {
		checkRequiredParametersPresent();

		runTask();
	}

	protected abstract void runTask() throws Exception;

	public abstract String[] getRequiredParameters();
}
