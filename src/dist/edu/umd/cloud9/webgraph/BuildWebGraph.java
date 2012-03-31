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
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;

/**
 * 
 * @author Nima Asadi
 *
 */
public class BuildWebGraph extends PowerTool {
	private static final Logger LOG = Logger.getLogger(BuildWebGraph.class);
	
	public static class Map extends MapReduceBase implements
		Mapper<IntWritable, ArrayListWritable<AnchorText>, IntWritable, ArrayListWritable<AnchorText>> {
		
		private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
		private static final IntWritable keyWord = new IntWritable();
		
		private static byte flag;
		
		public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
				OutputCollector<IntWritable, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			for(AnchorText data : anchors) {
				
				if(data.isURL()) {
					arrayList.clear();
					arrayList.add(data.clone());
					output.collect(key, arrayList);
				}
				
				if(!data.isExternalInLink() && !data.isInternalInLink())
					continue;
				
				//set the flag to "outgoing link"
				flag = data.isExternalInLink() ? AnchorTextConstants.Type.EXTERNAL_OUT_LINK.val :
													AnchorTextConstants.Type.INTERNAL_OUT_LINK.val;
				
				arrayList.clear();
				arrayList.add(new AnchorText(flag, AnchorTextConstants.EMPTY_STRING, key.get()));
				for(int source : data) {
					keyWord.set(source);
					output.collect(keyWord, arrayList);
				}
					
			}
			
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<IntWritable, ArrayListWritable<AnchorText>, IntWritable, ArrayListWritable<AnchorText>> {
	
		private static final ArrayListWritable<AnchorText> arrayList = 
			new ArrayListWritable<AnchorText>();
		private static ArrayListWritable<AnchorText> packet;
		private static boolean pushed;
		private static int outdegree;
		
		public void reduce(IntWritable key, Iterator<ArrayListWritable<AnchorText>> values,
				OutputCollector<IntWritable, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			arrayList.clear();
			outdegree = 0;
			
			while(values.hasNext()) {
				packet = values.next();
				
				for(AnchorText data : packet) {
				  
				  outdegree += data.getSize();
					
					pushed = false;
					
					for(int i = 0; i < arrayList.size(); i++) {
						if(arrayList.get(i).equalsIgnoreSources(data)) {
							arrayList.get(i).addDocumentsFrom(data);
							pushed = true;
							break;
						}
					}
					
					if(!pushed)
						arrayList.add(data.clone());
				}
			}
			
			arrayList.add(new AnchorText(AnchorTextConstants.Type.OUT_DEGREE.val, AnchorTextConstants.EMPTY_STRING, outdegree));
			
			Collections.sort(arrayList);
			output.collect(key, arrayList);
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

	public BuildWebGraph(Configuration conf) {
		super(conf);
	}


	public int runTool() throws Exception {

		JobConf conf = new JobConf(getConf(), BuildWebGraph.class);
		FileSystem fs = FileSystem.get(conf);
		
		int numMappers = conf.getInt("Cloud9.Mappers", 1);
		int numReducers = conf.getInt("Cloud9.Reducers", 200);

		String inputPath = conf.get("Cloud9.InputPath");
		String outputPath = conf.get("Cloud9.OutputPath");
		
		conf.setJobName("ConstructWebGraph");
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(ArrayListWritable.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

		SequenceFileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		LOG.info("BuildWebGraph");
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
