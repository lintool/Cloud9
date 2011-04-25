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

package edu.umd.cloud9.webgraph;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextTarget;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * @author Nima Asadi
 * @author Tamer Elsayed
 * 
 */
@SuppressWarnings("deprecation")
public class BuildAnchorTextInvertedIndex extends PowerTool {
	private static final Logger LOG = Logger.getLogger(BuildAnchorTextInvertedIndex.class);
	{
		LOG.setLevel(Level.INFO);
	}

	private static class MyMapper extends MapReduceBase implements
	Mapper<IntWritable, ArrayListWritable<AnchorText>, Text, AnchorTextTarget> {

		private static final AnchorTextTarget anchorTextTarget = new AnchorTextTarget();
		private static final Text keyOut = new Text();
		
		public void configure(JobConf job) {
			
		}

		public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
				OutputCollector<Text, AnchorTextTarget> output, Reporter reporter) throws IOException {
			anchorTextTarget.setTarget(key.get());
			
			for(AnchorText anchor : anchors) {
				
				if(!anchor.isExternalInLink() && !anchor.isInternalInLink())
					continue;
				
				keyOut.set(anchor.getText());
				anchorTextTarget.setWeight(anchor.getWeight());
				anchorTextTarget.setSources(new ArrayListOfIntsWritable(anchor.getSources()));
				
				output.collect(keyOut, anchorTextTarget);
			}

		}
	}

	private static class MyReducer extends MapReduceBase implements
	Reducer<Text, AnchorTextTarget, Text, ArrayListWritable<AnchorTextTarget>> {

		private static final ArrayListWritable<AnchorTextTarget> outList = 
			new ArrayListWritable<AnchorTextTarget>();
		
		private static AnchorTextTarget next;
		private static boolean pushed;

		public void reduce(Text anchorText, Iterator<AnchorTextTarget> values,
				OutputCollector<Text, ArrayListWritable<AnchorTextTarget>> output, Reporter reporter)
		throws IOException {
			
			outList.clear();
			while (values.hasNext()) {
				next = values.next();
				
				pushed = false;
				
				for(int i = 0; i < outList.size(); i++) {
					if(outList.get(i).equals(next)) {
						outList.get(i).addSources(next.getSources());
						pushed = true;
						break;
					}
				}
				
				if(!pushed)
					outList.add(new AnchorTextTarget(next));
			}

			Collections.sort(outList);
			output.collect(anchorText, outList);
		}
	}

	public static final String[] RequiredParameters = { 
		"Ivory.NumMapTasks",
		"Ivory.NumReduceTasks",
		"Ivory.InputPath", 
		"Ivory.OutputPath",
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildAnchorTextInvertedIndex(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {

		JobConf conf = new JobConf(getConf(), BuildAnchorTextInvertedIndex.class);
		FileSystem fs = FileSystem.get(conf);
			
		String inPath = conf.get("Ivory.InputPath");
		String outPath = conf.get("Ivory.OutputPath");

		Path inputPath = new Path(inPath);
		Path outputPath = new Path(outPath);

		int mapTasks = conf.getInt("Ivory.NumMapTasks", 0);
		int reduceTasks = conf.getInt("Ivory.NumReduceTasks", 0);

		if (fs.exists(outputPath)) {
			LOG.info("Index already exist: no indexing will be performed.");
			return 0;
		}

		conf.setJobName("BuildAnchorTextInvertedIndex");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(AnchorTextTarget.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ArrayListWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}
}
