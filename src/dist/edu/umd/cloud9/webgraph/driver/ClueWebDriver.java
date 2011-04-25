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

package edu.umd.cloud9.webgraph.driver;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.webgraph.BuildInverseWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.CollectHostnames;
import edu.umd.cloud9.webgraph.ComputeWeight;
import edu.umd.cloud9.webgraph.ExtractLinks;

/**
 * <p>
 * Main driver program for extracting the ClueWeb09 web graph, inverted web graph,
 * and lines of anchor text. Command-line arguments are as follows:
 * </p>
 * 
 * <ul>
 * <li>[collection-base-path]: the base path to the ClueWeb09 collection</li>
 * <li>[output-base-path]: the base path under which the output would be stored</li>
 * <li>[docno-mapping-file]: the path to the docno-mapping.dat file</li>
 * <li>[from-segment]: starting segment number</li>
 * <li>[to-segment]: ending segment number</li>
 * <li>[includeInternalLinks?]: 1 for including the internal links (i.e., links within a domain), 0 for not</li>
 * <li>[compute-default-anchor-weight?]: 1 to compute the default weights for lines of external anchor text, 0 for not</li>
 * <li>[normalizer] A normalizer class used to normalize the lines of anchor text, 
 * 					must extend *.anchor.normalize.AnchorTextNormalizer.</li>
 * </ul>
 * 
 * <p>
 * The starting and ending segments will correspond to paths
 * <code>/[collection-base-path]/en.XX</code> to <code>/[collection-base-path]/en.YY</code>.
 * </p>
 * 
 * <p>
 * The default weight used in this program was originally proposed by Metzler et. al in the following paper: <br />
 * 
 * D. Metzler, J. Novak, H. Cui, and S. Reddy. Building enriched document representations using aggregated 
 * anchor text. <i>In Proc. 32nd Annual International ACM SIGIR Conference on Research and Development in
 * Information Retrieval</i>, pages 219{226, New York, NY, USA, 2009. ACM.
 * </p>
 * 
 * @author Nima Asadi
 * 
 */

public class ClueWebDriver extends Configured implements Tool {
	
		//raw link information is stored at /base/path/extracted.links
		public static final String outputExtractLinks = "extracted.links";
		
		//inverted web graph w/ lines of anchor text is stored at /base/path/inverseWebGraph
		public static final String outputInverseWebGraph = "inverseWebGraph";
		
		//web graph is stored at /base/path/webGraph
		public static final String outputWebGraph = "webGraph";
		
		//hostname information (for computing default weights) is stored at /base/path/hostnames
		public static final String outputHostnames = "hostnames"; 
		
		//inverted web graph w/ weighted lines of anchor text is stored at /base/path/weightedInverseWebGraph 
		public static final String outputWeightedInverseWebGraph = "weightedInverseWebGraph";
			
		private static int printUsage() {
			System.out.println("usage: [collection-path] [output-base] [docno-mapping] "
					+ "[from-segment] [to-segment] [include-internal-links?] "
					+ "[compute-default-anchor-weights?] [normalizer]");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
	
		public int run(String[] args) throws Exception {
			
			if(args.length != 8) {
				printUsage();
				return -1;
			}
		
			Configuration conf = new Configuration();
			
			final String inputBase = args[0].endsWith("/") ? args[0] : args[0] + "/";
			final String outputBase = args[1].endsWith("/") ? args[1] : args[1] + "/";
			final String docnoMapping = args[2];
			final int fromSegment = Integer.parseInt(args[3]);
			final int toSegment = Integer.parseInt(args[4]);
			final boolean includeInternalLinks = Integer.parseInt(args[5]) == 1;
			final boolean computeAnchorWeights = Integer.parseInt(args[6]) == 1;
			final String normalizer = args[7];
			
			final int defaultReducers = 200;	//number of reducers per segment
			
			conf.setInt("Ivory.Mappers", 2000);
			conf.setInt("Ivory.Reducers", defaultReducers);
			conf.set("Ivory.DocnoMappingFile", docnoMapping);		
			conf.setBoolean("Ivory.IncludeInternalLinks", includeInternalLinks);
			conf.set("Ivory.AnchorTextNormalizer", normalizer);		
						
			//Extract link information for each segment separately
			for(int i = fromSegment; i <= toSegment; i++) {
				String inputPath = inputBase + "en." + (i == 10 ? "10" : ("0" + i));
				String outputPath = outputBase + outputExtractLinks + "/en." + (i == 10 ? "10" : ("0" + i));
				
				conf.set("Ivory.InputPath", inputPath);
				conf.set("Ivory.OutputPath", outputPath);
			
				int r = new ExtractLinks(conf).run();
				
				if(r != 0)
					return -1;
			}
			
			//Construct the inverse web graph (i.e., collect incoming link information)
			String inputPath = "";
			for (int i = fromSegment; i < toSegment; i++) {
				inputPath += outputBase + outputExtractLinks + "/en.0" + i + "/,";
			}
	
			if (toSegment == 10) {
				inputPath +=  outputBase + outputExtractLinks + "/en.10/";
			} else {
				inputPath += outputBase + outputExtractLinks + "/en.0" + toSegment + "/";
			}
			
			String outputPath = outputBase +  outputInverseWebGraph + "/";
			
			conf.set("Ivory.InputPath", inputPath);
			conf.set("Ivory.OutputPath", outputPath);
			conf.setInt("Ivory.Mappers", 1);
			conf.setInt("Ivory.Reducers", defaultReducers * (toSegment - fromSegment + 1));
	
			int r = new BuildInverseWebGraph(conf).run();
			if(r != 0)
				return -1;
			
			
			//Construct the web graph
			inputPath = outputBase + outputInverseWebGraph + "/";
			outputPath = outputBase +  outputWebGraph + "/";
			
			conf.set("Ivory.InputPath", inputPath);
			conf.set("Ivory.OutputPath", outputPath);
			conf.setInt("Ivory.Mappers", 1);
			conf.setInt("Ivory.Reducers", defaultReducers * (toSegment - fromSegment + 1));
			r = new BuildWebGraph(conf).run();
			if(r != 0)
				return -1;
			
			if(computeAnchorWeights) {
				//Propagating domain names in order to compute anchor weights
				inputPath = outputBase + outputWebGraph + "/";
				outputPath = outputBase +  outputHostnames + "/";
				
				conf.set("Ivory.InputPath", inputPath);
				conf.set("Ivory.OutputPath", outputPath);
				conf.setInt("Ivory.Mappers", 1);
				conf.setInt("Ivory.Reducers", defaultReducers * (toSegment - fromSegment + 1));
				
				r = new CollectHostnames(conf).run();
				if(r != 0)
					return -1;
				
				//Compute the weights
				inputPath = outputBase + outputInverseWebGraph + "/," + outputBase + outputHostnames + "/";
				outputPath = outputBase +  outputWeightedInverseWebGraph + "/";
				
				conf.set("Ivory.InputPath", inputPath);
				conf.set("Ivory.OutputPath", outputPath);
				conf.setInt("Ivory.Mappers", 1);
				conf.setInt("Ivory.Reducers", defaultReducers * (toSegment - fromSegment + 1));
				
				r = new ComputeWeight(conf).run();
				if(r != 0)
					return -1;
			}
			
			return 0;
		}
	
		public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new ClueWebDriver(), args);
			System.exit(res);
		}
}
