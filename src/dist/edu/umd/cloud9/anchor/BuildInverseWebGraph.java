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

package edu.umd.cloud9.anchor;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.log4j.Logger;

import edu.umd.cloud9.anchor.data.AnchorText;
import edu.umd.cloud9.anchor.data.AnchorTextConstants;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;

/**
 * 
 * @author Nima Asadi
 *
 */

@SuppressWarnings("deprecation")
public class BuildInverseWebGraph extends PowerTool {

	private static final Logger sLogger = Logger.getLogger(BuildInverseWebGraph.class);

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, ArrayListWritable<AnchorText>, IntWritable, ArrayListWritable<AnchorText>> {

		private static final IntWritable keyWord = new IntWritable();
		private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
		
		private static ArrayListWritable<AnchorText> packet;
		private static boolean pushed;
		
		private int indegree, outdegree;
		private ArrayList<Integer> docnos = new ArrayList<Integer>();
		

		public void reduce(Text key, Iterator<ArrayListWritable<AnchorText>> values,
				OutputCollector<IntWritable, ArrayListWritable<AnchorText>> output, Reporter reporter) throws IOException {
			
			docnos.clear();
			arrayList.clear();
			indegree = 0;
			outdegree = 0;
			
			while(values.hasNext()) {
				packet = values.next();
				
				for(AnchorText data : packet) {
					
					//outdegree data
					if(data.isOutDegree()) {
						for(int degree: data)	//in theory, there must be only one "outdegree" packet. Unless there are duplicate pages
							outdegree = degree;
						continue;
					}
					
					//docno field data
					if(data.isDocnoField()) {
						for(int docno: data)	//again, in theory, there must be only one "outdegree" packet. Unless there are duplicate pages.
							docnos.add(docno);
						
						continue;
					}
					
					pushed = false;
					
					indegree += data.getSize();
					
					for(int i = 0; i < arrayList.size(); i++)
						if(arrayList.get(i).equalsIgnoreSources(data)) {
							arrayList.get(i).addSourcesFrom(data);
							pushed = true;
							break;
						}
					
					if(!pushed)
						arrayList.add(data.clone());
				}
						
			}
			
			arrayList.add(new AnchorText(AnchorTextConstants.IN_DEGREE, AnchorTextConstants.EMPTY_STRING, indegree));
			arrayList.add(new AnchorText(AnchorTextConstants.OUT_DEGREE, AnchorTextConstants.EMPTY_STRING, outdegree));
			arrayList.add(new AnchorText(AnchorTextConstants.URL_FIELD, key.toString()));
			
			Collections.sort(arrayList);
			
			//if there was no document number detected, this record would not be emitted.  
			for(int docno : docnos) {
				keyWord.set(docno);
				output.collect(keyWord, arrayList);
			}

		}

		

	}
	
	public static final String[] RequiredParameters = {
		"Ivory.InputPath",
		"Ivory.OutputPath",
		"Ivory.Mappers",
		"Ivory.Reducers"
	};

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public BuildInverseWebGraph(Configuration conf) {
		super(conf);
	}


	public int runTool() throws Exception {

		JobConf conf = new JobConf(getConf(), BuildInverseWebGraph.class);
		FileSystem fs = FileSystem.get(conf);
		
		int numMappers = conf.getInt("Ivory.Mappers", 1);
		int numReducers = conf.getInt("Ivory.Reducers", 200);

		String inputPath = conf.get("Ivory.InputPath");
		String outputPath = conf.get("Ivory.OutputPath");
		
		conf.setJobName("InverseWebGraph");
		conf.set("mapred.child.java.opts", "-Xmx4096m");
		conf.setInt("mapred.task.timeout", 60000000);

		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);

		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(ArrayListWritable.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

		SequenceFileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		sLogger.info("BuildInverseWebGraph");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);		

		if(!fs.exists(new Path(outputPath)))
			JobClient.runJob(conf);
		else
			sLogger.info(outputPath + " already exists! Skipping this step...");

		return 0;
	}

}
