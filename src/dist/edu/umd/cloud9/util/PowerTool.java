package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class PowerTool extends Configured implements Tool {

	public PowerTool(Configuration conf) {
		super(conf);
	}

	public int run() throws Exception {
		checkRequiredParametersPresent();
		return runTool();
	}

	public int run(String[] args) throws Exception {
		checkRequiredParametersPresent();
		return runTool();
	}

	private void checkRequiredParametersPresent() {
		if (getRequiredParameters() == null)
			throw new RuntimeException("Error: getRequiredParameters() returns null!");

		Configuration conf = getConf();
		for (String param : getRequiredParameters()) {
			if (conf.get(param) == null) {
				throw new RuntimeException("Error: required parameter \"" + param + "\" not defined");
			}
		}
	}

	public abstract String[] getRequiredParameters();

	public abstract int runTool() throws Exception;
}
