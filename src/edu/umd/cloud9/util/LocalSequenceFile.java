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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class LocalSequenceFile {

	private LocalSequenceFile() {
	}

	public static SequenceFile.Reader createReader(String file) {
		JobConf config = new JobConf();
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(FileSystem.get(config), new Path(
					file), config);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return reader;
	}

}
