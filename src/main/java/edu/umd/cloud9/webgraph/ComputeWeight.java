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
import java.util.HashSet;
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
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;

/**
 * 
 * @author Nima Asadi
 *
 */
public class ComputeWeight extends PowerTool {

	private static final Logger LOG = Logger.getLogger(ComputeWeight.class);
	private static final int HOSTMAP = 1;
	private static final int DATA = 0;
	
	public static class Map extends MapReduceBase implements
	Mapper<IntWritable, ArrayListWritable<AnchorText>, PairOfInts, ArrayListWritable<AnchorText>> {
	
		private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
		private static final PairOfInts keyWord = new PairOfInts();
				
		public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
				OutputCollector<PairOfInts, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			if(anchors.size() == 0)	//not a valid record
				return;			

			if(!anchors.get(0).isOfOtherTypes()) {
				keyWord.set(key.get(), DATA);
				output.collect(keyWord, anchors);
				return;
			}

			keyWord.set(key.get(), HOSTMAP);
			
			for(AnchorText data : anchors) {
				arrayList.clear();
				arrayList.add(data);
				output.collect(keyWord, arrayList);
			}
		}
	}
	
	protected static class Partition implements Partitioner<PairOfInts, ArrayListWritable<AnchorText>> {
		public void configure(JobConf job) {
		}

		public int getPartition(PairOfInts key, ArrayListWritable<AnchorText> value, int numReduceTasks) {
			return Math.abs(key.getLeftElement() % numReduceTasks);
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<PairOfInts, ArrayListWritable<AnchorText>, IntWritable, ArrayListWritable<AnchorText>> {
			
		private static OutputCollector<IntWritable, ArrayListWritable<AnchorText>> outputCollector;
		
		private static final ArrayListWritable<AnchorText> arrayList = 
			new ArrayListWritable<AnchorText>();
		private static final IntWritable keyWord = new IntWritable();
		
		private static ArrayListWritable<AnchorText> packet;
		
		private static int currentDocument, linkCounter;
		private static boolean firstTime = true;
		private static String lastHost;
		private static HashSet<Integer> intersects = new HashSet<Integer>();
		
		private static int[] simMap;
		
		public void reduce(PairOfInts key, Iterator<ArrayListWritable<AnchorText>> values,
				OutputCollector<IntWritable, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			//should receive the DATA packet first, which contains the anchor text information
			if(key.getRightElement() == DATA) {
				
				if(!firstTime) {
					keyWord.set(currentDocument);
					output.collect(keyWord, arrayList);
				} else {
					firstTime = false;
					outputCollector = output;
				}
				
				currentDocument = key.getLeftElement();
				arrayList.clear();
				
				//should run only once, since there is only one list of anchor text per page
				while(values.hasNext()) {
					packet = values.next();
					arrayList.ensureCapacity(packet.size());
					
					for(AnchorText data : packet)
						arrayList.add(data);
				}
				
			} else {
				
				//in case there are multiple packets for each line of anchor text, 
				//we build a table that maps each packet to a number. If two packets 
				//have the same anchor text, their numbers will be equal.
				linkCounter = 0;
				simMap = new int[arrayList.size()];
				for(int i = 1; i < arrayList.size(); i++) {
					if(arrayList.get(i).equalsIgnoreSources(arrayList.get(i - 1))) {
						simMap[i] = simMap[i-1];
					} else {
						simMap[i] = i;
						
						if(arrayList.get(i).isExternalInLink())
							linkCounter++;
					}
				}
				lastHost = "";
				intersects.clear();
				
				while(values.hasNext()) {
					packet = values.next();
					
					for(AnchorText data : packet) {	//should run only once (refer to the mapper class)
						
						if(!data.getText().equals(lastHost)) {
							
							if(intersects.size() > 0) {
								for(int i = 0; i < simMap.length; i++) {
									if(intersects.contains(simMap[i])) {
										arrayList.get(i).setWeight(arrayList.get(i).getWeight() + (1.0f / intersects.size()));
									}
								}
							}
							
							intersects.clear();
						}
						
						lastHost = data.getText();
						
						for(int i = 0; i < arrayList.size(); i++) {
							if(!arrayList.get(i).isExternalInLink())
								continue;
							
							if(intersects.contains(simMap[i]))
								continue;
						
							//if there is only one line of anchor text, then definitely it intersects 
							//with any host map associated with this object
							if(linkCounter == 1) {
								intersects.add(simMap[i]);
							} else if(data.intersects(arrayList.get(i))) {
								intersects.add(simMap[i]);
							}
						}
					}
				}
				
				if(intersects.size() > 0) {
					for(int i = 0; i < simMap.length; i++) {
						if(intersects.contains(simMap[i])) {
							arrayList.get(i).setWeight(arrayList.get(i).getWeight() + (1.0f / intersects.size()));
						}
					}
				}
				
				intersects.clear();
			}
		}
		
		public void close() throws IOException {
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

	public ComputeWeight(Configuration conf) {
		super(conf);
	}


	public int runTool() throws Exception {

		JobConf conf = new JobConf(getConf(), ComputeWeight.class);
		FileSystem fs = FileSystem.get(conf);
		
		int numMappers = conf.getInt("Cloud9.Mappers", 1);
		int numReducers = conf.getInt("Cloud9.Reducers", 200);

		String inputPath = conf.get("Cloud9.InputPath");
		String outputPath = conf.get("Cloud9.OutputPath");
		
		conf.setJobName("ComputeWeights");
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);

		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(Partition.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		
		conf.setMapOutputKeyClass(PairOfInts.class);
		conf.setMapOutputValueClass(ArrayListWritable.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

		SequenceFileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		LOG.info("ComputeWeight");
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + outputPath);		

		if(!fs.exists(new Path(outputPath))) {
			JobClient.runJob(conf);
		} else {
			LOG.info(outputPath + " already exists! Skipping this step...");
		}

		return 0;
	}
}
