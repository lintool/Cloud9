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
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;

/**
 * 
 * @author Nima Asadi
 *
 */
public class CollectHostnames extends PowerTool {
	private static final Logger sLogger = Logger.getLogger(CollectHostnames.class);
	
	public static class Map extends MapReduceBase implements
		Mapper<IntWritable, ArrayListWritable<AnchorText>, PairOfIntString, IntWritable> {
		
		private static final PairOfIntString keyWord = new PairOfIntString();
		private static final IntWritable valueWord = new IntWritable();
		
		private static String host;
		
		public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
				OutputCollector<PairOfIntString, IntWritable> output, Reporter reporter) throws IOException {
						
			for(AnchorText data : anchors) {
				if(data.isURL()) 
					try {
						//extract the hostname for a given URL
						host = new URI(data.getText()).getHost();
					}catch(Exception e) {
						return;
					}
			}
			
			for(AnchorText data : anchors) {
				
				if(!data.isExternalOutLink())
					continue;
					
				valueWord.set(key.get());
				for(int target : data) {
					keyWord.set(target, host);
					output.collect(keyWord, valueWord);
				}
					
			}
			
		}
	}
	
	protected static class Partition implements Partitioner<PairOfIntString, IntWritable> {
		public void configure(JobConf job) {
		}

		public int getPartition(PairOfIntString key, IntWritable value, int numReduceTasks) {
			return Math.abs(key.getLeftElement() % numReduceTasks);
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<PairOfIntString, IntWritable, IntWritable, ArrayListWritable<AnchorText>> {
	
		private static OutputCollector<IntWritable, ArrayListWritable<AnchorText>> outputCollector;
		private static final ArrayListWritable<AnchorText> arrayList = 
			new ArrayListWritable<AnchorText>();
		private static final IntWritable keyWord = new IntWritable();

		private static boolean firstTime = true;
		private static int currentDocument, packet;
		
		public void reduce(PairOfIntString key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			if(firstTime) {
				outputCollector = output;
				firstTime = false;
				arrayList.clear();
				currentDocument = key.getLeftElement();
			} else if(currentDocument != key.getLeftElement()) {
				Collections.sort(arrayList);
				keyWord.set(currentDocument);
				output.collect(keyWord, arrayList);
				
				currentDocument = key.getLeftElement();
				arrayList.clear();
			}
			
			arrayList.add(new AnchorText(AnchorTextConstants.Type.OTHER_TYPES.val, key.getRightElement()));
			int currentIndex = arrayList.size() - 1;
			
			while(values.hasNext()) {
				packet = values.next().get();
				
				//break larger chunks of data to smaller packets - reduces the underlying HashMap costs
				if(arrayList.get(currentIndex).getSize() < AnchorTextConstants.MAXIMUM_SOURCES_PER_PACKET) {
					arrayList.get(currentIndex).addDocument(packet);
				} else {
					arrayList.add(new AnchorText(AnchorTextConstants.Type.OTHER_TYPES.val, key.getRightElement(), packet));
					currentIndex = arrayList.size() - 1;
				}
			}
		}
		
		public void close() throws IOException {
			Collections.sort(arrayList);
			keyWord.set(currentDocument);
			outputCollector.collect(keyWord, arrayList);
		}
	}
	
	public static final String[] RequiredParameters = {
		"Cloud9.InputPath",
		"Cloud9.OutputPath",
		"Cloud9.Mappers",
		"Cloud9.Reducers"
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public CollectHostnames(Configuration conf) {
		super(conf);
	}

	public int runTool() throws Exception {

		JobConf conf = new JobConf(getConf(), CollectHostnames.class);
		FileSystem fs = FileSystem.get(conf);
		
		int numMappers = conf.getInt("Cloud9.Mappers", 1);
		int numReducers = conf.getInt("Cloud9.Reducers", 200);

		String inputPath = conf.get("Cloud9.InputPath");
		String outputPath = conf.get("Cloud9.OutputPath");
		
		conf.setJobName("CollectHostnames");
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);

		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(Partition.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		
		conf.setMapOutputKeyClass(PairOfIntString.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

		SequenceFileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		sLogger.info("PropagateHostname");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);		

		if(!fs.exists(new Path(outputPath))) {
			JobClient.runJob(conf);
		} else {
			sLogger.info(outputPath + " already exists! Skipping this step...");
		}

		return 0;
	}
}
