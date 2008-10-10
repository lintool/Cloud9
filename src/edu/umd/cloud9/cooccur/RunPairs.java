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

package edu.umd.cloud9.cooccur;

import org.apache.hadoop.conf.Configuration;

/**
 * Driver for {@link ComputeCooccurrenceMatrixPairs}.
 */
public class RunPairs {

	private RunPairs(){
	}
	
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
