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

package edu.umd.cloud9.io;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;


import edu.umd.cloud9.io.pair.PairOfWritables;


public class ReadSequenceFileEntries {


	public static Map <Writable, Writable> convertListToMap (List <PairOfWritables <Writable, Writable>> entries) {
		Map <Writable, Writable> newMap = new HashMap <Writable, Writable> ();
		for (PairOfWritables <Writable, Writable> entry : entries) {
			newMap.put (entry.getKey (), entry.getValue ());
		}
		return newMap;
	}


	private ReadSequenceFileEntries () {
	}
	
	private static List <PairOfWritables <Writable, Writable>> readSequenceFile (Path path, int max) {
		List <PairOfWritables <Writable, Writable>> entries = new ArrayList <PairOfWritables <Writable, Writable>> ();

		try {
			Configuration config = new Configuration ();
			SequenceFile.Reader reader = new SequenceFile.Reader (FileSystem.get (config), path, config);

			Writable key, value;
			int n = 0;
			key = (Writable) reader.getKeyClass ().newInstance ();
			value = (Writable) reader.getValueClass ().newInstance ();

			while (reader.next (key, value)) {
				PairOfWritables <Writable, Writable> entry = new PairOfWritables <Writable, Writable> (key, value);
				entries.add (entry);
				key = (Writable) reader.getKeyClass ().newInstance ();
				value = (Writable) reader.getValueClass ().newInstance ();
				n += 1;
				if (n >= max)
					break;
			}
			reader.close ();
		} catch (Exception e) {
			e.printStackTrace ();
		}

		return entries;
	}

	private static List <PairOfWritables <Writable, Writable>> readSequenceFilesInDir (Path path, int max) {
		List <PairOfWritables <Writable, Writable>> entries = new ArrayList <PairOfWritables <Writable, Writable>> ();
		JobConf config = new JobConf();
		int n = 0;
		try {
			FileSystem fileSys = FileSystem.get(config);
			FileStatus[] stat = fileSys.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {
				// System.out.println ("looking at: " + stat[i].getPath().toString ());
				if (stat[i].getPath().getName ().startsWith ("part-"))
					entries.addAll (readSequenceFile(stat[i].getPath(), max));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return entries;
	}

	public static List <PairOfWritables <Writable, Writable>> readSequencePath (Path path) {
		return readSequencePath (path, Integer.MAX_VALUE);
	}

	public static List <PairOfWritables <Writable, Writable>> readSequencePath (Path path, int max) {
		boolean readingDir = false;
		try {
			Configuration config = new JobConf();
			FileSystem fileSys = FileSystem.get(config);
			readingDir = fileSys.getFileStatus(path).isDir();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (readingDir)
			return readSequenceFilesInDir (path, max);
		else
			return readSequenceFile (path, max);
	}

	public static void main (String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("args: [path]");
			System.exit(-1);
		}

		String f = args[0];

		int max = Integer.MAX_VALUE;
		if (args.length >= 2) {
			max = Integer.parseInt(args[1]);
		}

		String question = args [2];

		Path p = new Path(f);

		List <PairOfWritables <Writable, Writable>> entries = readSequencePath (p, max);
		System.out.println (entries.size () + " entries");
		PairOfWritables <Writable, Writable> firstElt = entries.get (0);
		System.out.println ("first key class: " + firstElt.getKey ().getClass ());
		System.out.println ("first value class: " + firstElt.getValue ().getClass ());

		if (question.equals ("keys")) {
			int i = 0;
			for (PairOfWritables <Writable, Writable> entry : entries) {
				System.out.println ("key [" + i + "]: " + entry.getKey ());
				i++;
				if (i == max) return;
			}
		}
		else if (question.equals ("both")) {
			int i = 0;
			for (PairOfWritables <Writable, Writable> entry : entries) {
				System.out.println ("key [" + i + "]: " + entry.getKey () + ", value [" + i + "]: " + entry.getValue ());
				i++;
				if (i == max) return;
			}
		}
	}
}
