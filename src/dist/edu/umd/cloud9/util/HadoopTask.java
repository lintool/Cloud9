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

package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

/**
 * <p>
 * An abstract class representing a generic Hadoop task. <b>NOTE:</b> I wrote
 * this class a long time ago, before I fully understood the Hadoop API (and
 * this class has hung around since); similar functionality is already provided
 * by Hadoop's {@link Tool} interface. Use {@link PowerTool} instead.
 * </p>
 * 
 * <p>
 * This class provides a way to package together one or more MapReduce job in a
 * common parameter-passing interface. The standard way to invoke a HadoopTask
 * is:
 * </p>
 * 
 * <pre>
 * Configuration config = new Configuration();
 * config.set(&quot;param1&quot;, value1);
 * config.set(&quot;param2&quot;, value2);
 * ...		
 * ConcreteHadoopTask task = new ConcreteHadoopTask(config);
 * task.run();
 * </pre>
 * 
 * <p>
 * To implement a concrete HadoopTask, extend this class and implement
 * <code>runTask()</code> and <code>getRequiredParameters()</code>:
 * </p>
 * 
 * <pre>
 * public static final String[] RequiredParameters = { &quot;a&quot;, &quot;b&quot;, &quot;c&quot; };
 * 
 * public String[] getRequiredParameters() {
 * 	return RequiredParameters;
 * }
 * 
 * public void runTask() throws Exception {
 * 	// set up and run the MapReduce job
 * }
 * </pre>
 * 
 * <p>
 * Code in the abstract HadoopTask class will handle checking to make sure all
 * required parameters are present.
 * </p>
 * 
 * @author Jimmy Lin
 */
@Deprecated
public abstract class HadoopTask implements Configurable {

	private Configuration mConf;

	/**
	 * Creates a HadoopTask.
	 * 
	 * @param conf
	 */
	public HadoopTask(Configuration conf) {
		mConf = conf;

		if (getRequiredParameters() == null)
			throw new RuntimeException("Error: getRequiredParameters() returns null!");
	}

	private void checkRequiredParametersPresent() {
		if (getRequiredParameters() == null)
			throw new RuntimeException("Error: getRequiredParameters() returns null!");

		for (String param : getRequiredParameters()) {
			if (mConf.get(param) == null) {
				throw new RuntimeException("Error: required parameter '" + param + "' not defined");
			}
		}
	}

	/**
	 * Returns the Configuration object associated with this HadoopTask.
	 * 
	 * @return the Configuration object associated with this HadoopTask
	 */
	public Configuration getConf() {
		return mConf;
	}

	/**
	 * Sets the Configuration object associated with this HadoopTask.
	 * 
	 * @param conf
	 *            the Configuration object associated with this HadoopTask
	 */
	public void setConf(Configuration conf) {
		mConf = conf;
	}

	/**
	 * Runs this HadoopTask
	 * 
	 * @throws Exception
	 */
	public void run() throws Exception {
		checkRequiredParametersPresent();

		runTask();
	}

	/**
	 * Called by <code>run()</code> after verifying that required parameters
	 * are present. This is an abstract method that must be implemented by the
	 * concrete class.
	 * 
	 * @throws Exception
	 */
	public abstract void runTask() throws Exception;

	/**
	 * Returns the required parameters for this HadoopTask. This is an abstract
	 * method that must be implemented by the concrete class.
	 * 
	 * @return array of Strings containing the required parameters for this
	 *         HadoopTask
	 */
	public abstract String[] getRequiredParameters();
}
