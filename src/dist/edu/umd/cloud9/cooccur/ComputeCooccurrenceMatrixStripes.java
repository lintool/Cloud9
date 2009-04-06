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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.io.OHMapSIW;
import edu.umd.cloud9.util.HadoopTask;

/**
 * <p>
 * Implementation of the "stripes" algorithm for computing co-occurrence
 * matrices from corpora. Algorithm is described in:
 * </p>
 * 
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the
 * Masses: A Case Study in Computing Word Co-occurrence Matrices with MapReduce.</b>
 * <i>Proceedings of the 2008 Conference on Empirical Methods in Natural
 * Language Processing (EMNLP 2008).</i></blockquote>
 */
public class ComputeCooccurrenceMatrixStripes extends HadoopTask {

	private static int mWindow = 2;

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, OHMapSIW> {

		public void map(LongWritable key, Text line, OutputCollector<Text, OHMapSIW> output,
				Reporter reporter) throws IOException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// skip empty tokens
				if (term.length() == 0)
					continue;

				OHMapSIW map = new OHMapSIW();

				for (int j = i - mWindow; j < i + mWindow + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					// skip empty tokens
					if (terms[j].length() == 0)
						continue;

					if (map.containsKey(terms[j])) {
						map.increment(terms[j]);
					} else {
						map.put(terms[j], 1);
					}
				}

				output.collect(new Text(term), map);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, OHMapSIW, Text, OHMapSIW> {

		public void reduce(Text key, Iterator<OHMapSIW> values,
				OutputCollector<Text, OHMapSIW> output, Reporter reporter) throws IOException {

			OHMapSIW map = null;

			while (values.hasNext()) {
				if (map == null) {
					map = values.next();
				} else {
					map.plus(values.next());
				}
			}

			output.collect(key, map);
		}
	}

	public ComputeCooccurrenceMatrixStripes(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = { "CollectionName", "InputPath",
			"OutputPath", "NumMapTasks", "NumReduceTasks", "Window" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public void runTask() throws Exception {
		String collection = getConf().get("CollectionName");
		String inputPath = getConf().get("InputPath");
		String outputPath = getConf().get("OutputPath");

		int mapTasks = getConf().getInt("NumMapTasks", 0);
		int reduceTasks = getConf().getInt("NumReduceTasks", 0);
		mWindow = getConf().getInt("Window", 0);

		JobConf conf = new JobConf(ComputeCooccurrenceMatrixStripes.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		conf.setJobName("CooccurrenceMatrixStripes-" + collection);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		FileOutputFormat.setCompressOutput(conf, false);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(OHMapSIW.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

	}

}
