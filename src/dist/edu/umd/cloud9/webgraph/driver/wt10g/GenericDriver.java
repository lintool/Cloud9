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

package edu.umd.cloud9.webgraph.driver.wt10g;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.webgraph.BuildReverseWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.CollectHostnames;
import edu.umd.cloud9.webgraph.ComputeWeight;
import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

import edu.umd.cloud9.webgraph.driver.wt10g.CollectionConfigurationManager;

/**
 * <p>
 * Main driver program for extracting the web graph, reverse web graph, and
 * lines of anchor text. Command-line arguments are as follows:
 * </p>
 * 
 * <ul>
 * <li>collection-base-path: the base path to the collection</li>
 * <li>output-base-path: the base path under which the output would be stored</li>
 * <li>[-cn {clue|trecweb|trec|wt10g|gov2}]: the collection used</li>
 * <li>[-ci userSpecifiedInputFormatClass]: use user specified input format
 * class</li>
 * <li>[-cm userSpecifiedDocnoMappingClass]: use user specified docno mapping
 * class</li>
 * <li>[-f filter]: filter applied to files in collection base path</li>
 * <li>[-ss splitSize]: num of files in each split</li>
 * <li>[-il]: use this for including the internal links (i.e., links within a
 * domain); remove for not</li>
 * <li>[-caw]: use this to compute the default weights for lines of external
 * anchor text, remove for not</li>
 * <li>[-nm normalizer] A normalizer class used to normalize the lines of anchor
 * text, must extend *.anchor.normalize.AnchorTextNormalizer.</li>
 * <li>[<key:value> ..]: key-value pairs to put in configuration files. It shall
 * also be used as input method for user specified classes</li>
 * </ul>
 * 
 * <p>
 * The default weight used in this program was originally proposed by Metzler
 * et. al in the following paper: <br />
 * 
 * D. Metzler, J. Novak, H. Cui, and S. Reddy. Building enriched document
 * representations using aggregated anchor text. <i>In Proc. 32nd Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval</i>, pages 219{226, New York, NY, USA, 2009. ACM.
 * </p>
 * 
 * @author Nima Asadi , Modified by Fangyue Wang
 * 
 */

public class GenericDriver extends Configured implements Tool
{
	// raw link information is stored at /base/path/extracted.links
	public static final String outputExtractLinks = "extracted.links";

	// reverse web graph w/ lines of anchor text is stored at
	// /base/path/reverseWebGraph
	public static final String outputReverseWebGraph = "reverseWebGraph";

	// web graph is stored at /base/path/webGraph
	public static final String outputWebGraph = "webGraph";

	// hostname information (for computing default weights) is stored at
	// /base/path/hostnames
	public static final String outputHostnames = "hostnames";

	// reverse web graph w/ weighted lines of anchor text is stored at
	// /base/path/weightedReverseWebGraph
	public static final String outputWeightedReverseWebGraph = "weightedReverseWebGraph";

	

	private String inputBase;
	private String outputBase;
	private boolean includeInternalLinks = false;
	private boolean computeAnchorWeights = false;
	private String normalizer = "edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer";

	// private ArgumentManager moreArgs = new ArgumentManager();
	private String filtername = null;
	// private boolean userAutomaticSupport = true;
	// private String collection_name;
	final int defaultReducers = 200; // number of reducers per segment

	Configuration conf;
	CollectionConfigurationManager configer;

	public int run(String[] args) throws Exception
	{
		conf = getConf();
		configer = new CollectionConfigurationManager();

		if (!readInput(args))
		{
			printUsage();
			return -1;
		}

		configer.applyConfig(conf);
		
		conf.setInt("Cloud9.Mappers", 2000);
		conf.setInt("Cloud9.Reducers", defaultReducers);
		conf.setBoolean("Cloud9.IncludeInternalLinks", includeInternalLinks);
		conf.set("Cloud9.AnchorTextNormalizer", normalizer);

		// Job 1:
		// Extract link information for each segment separately
		String inputPath = inputBase;
		String outputPath = outputBase + "/" + outputExtractLinks;

		conf.set("Cloud9.InputPath", inputPath);
		conf.set("Cloud9.OutputPath", outputPath);

		int r = new GenericExtractLinks(conf, configer).run();

		if (r != 0)
		{
			return -1;
		}

		// Job 2:
		// Construct the reverse web graph (i.e., collect incoming link
		// information)
		inputPath = outputBase + "/" + outputExtractLinks;
		outputPath = outputBase + "/" + outputReverseWebGraph + "/";

		conf.set("Cloud9.InputPath", inputPath);
		conf.set("Cloud9.OutputPath", outputPath);
		conf.setInt("Cloud9.Reducers", defaultReducers);

		r = new BuildReverseWebGraph(conf).run();
		if (r != 0)
			return -1;

		// Job 3:
		// Construct the web graph
		inputPath = outputBase + "/" + outputReverseWebGraph + "/";
		outputPath = outputBase + "/" + outputWebGraph + "/";

		conf.set("Cloud9.InputPath", inputPath);
		conf.set("Cloud9.OutputPath", outputPath);
		conf.setInt("Cloud9.Mappers", 1);
		conf.setInt("Cloud9.Reducers", defaultReducers);
		r = new BuildWebGraph(conf).run();
		if (r != 0)
			return -1;

		if (computeAnchorWeights)
		{
			// Propagating domain names in order to compute anchor weights
			inputPath = outputBase + "/" + outputWebGraph + "/";
			outputPath = outputBase + "/" + outputHostnames + "/";

			conf.set("Cloud9.InputPath", inputPath);
			conf.set("Cloud9.OutputPath", outputPath);
			conf.setInt("Cloud9.Mappers", 1);
			conf.setInt("Cloud9.Reducers", defaultReducers);

			r = new CollectHostnames(conf).run();
			if (r != 0)
				return -1;

			// Compute the weights
			inputPath = outputBase + "/" + outputReverseWebGraph + "/,"
					+ outputBase + "/" + outputHostnames + "/";
			outputPath = outputBase + "/" + outputWeightedReverseWebGraph + "/";

			conf.set("Cloud9.InputPath", inputPath);
			conf.set("Cloud9.OutputPath", outputPath);
			conf.setInt("Cloud9.Mappers", 1);
			conf.setInt("Cloud9.Reducers", defaultReducers);

			r = new ComputeWeight(conf).run();
			if (r != 0)
			{
				return -1;
			}
		}

		return 0;
	}

	// assumption:
	// inputBase, outputBase, inputPath are all absolute path.
	// Abandoned
	// public String generateOutputPath(String inputPath, String secondPrefix)
	// {
	// return inputPath.replaceFirst(inputBase, outputBase + secondPrefix);
	// }

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner
				.run(new Configuration(), new GenericDriver(), args);
		System.exit(res);
	}

	private static int printUsage()
	{
		System.out.println("\nusage: collection-path output-base"
				+ "[-cn {clue|trecweb|gov2|wt10g}] "
				+ "[-ci userSpecifiedInputFormatClass] "
				+ "[-cm userSpecifiedDocnoMappingClass] "
				+ "-cf userSpecifiedDocnoMappingFile "
				// + "[-f filter] "
				// + "[-ss splitSize] "
				+ "[-il] " 
				+ "[-caw] " 
				+ "[-nm normalizerClass] "
				+ "[<key:value> ..]");

		System.out.println("Help:");

		System.out.println("collection-path\n\tinput directory");

		System.out.println("output-base\n\toutput directory");

		System.out
				.println("-cn {clue|trecweb|gov2|wt10g}\n\tname the collection name, if it is supported, automatic configuration will be applied");

		System.out
				.println("-ci userSpecifiedInputFormatClass\n\tspecify the class work as FileInputFormat; Required when -cn is not specified");

		System.out
				.println("-cm userSpecifiedDocnoMappingClass\n\tspecify the class work as DocnoMapping; Required when -cn is not specified. It should implement GenericDocnoMapping interface.");

		System.out
				.println("-cf userSpecifiedDocnoMappingFile\n\tspecify the File work as input to specified DocnoMapping class.");

		// System.out.println("-f <filtername>\n\tspecify regex filter to filter files in collection-path");
		// System.out.println("-ss <splitSize>\n\tspecify the number of data files processed together as a split.");
		System.out
				.println("-il\n\tinclude internal links, without this option we will not include internal links");

		System.out
				.println("-caw\n\tcompute default anchor weights, without this option we will not compute default anchor weights");

		System.out
				.println("-nm normalizerClass\n\ta normalizer class used to normalize the lines of anchor text, must extend *.anchor.normalize.AnchorTextNormalizer.");

		System.out
				.println("<key:value>\n\tAdditional key-value pairs that will be stored in configuration.");

		System.out
				.println("Note: <key:value>\n\tshould be used as the argument-passing method for any user specified class");

		System.out.println();

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	// class RegexFilter implements FilenameFilter
	// {
	// private Pattern pattern;
	//
	// public RegexFilter(String regex)
	// {
	// pattern = Pattern.compile(regex);
	// }
	//
	// public boolean accept(File dir, String name)
	// {
	// return pattern.matcher(name).matches();
	// }
	// }

	private boolean readInput(String[] args)
	{
		if (args.length < 6)
		{
			System.out.println("More arguments needed.");
			return false;
		}

		inputBase = new File(args[0]).getAbsolutePath();
		outputBase = new File(args[1]).getAbsolutePath();

		int argc = args.length - 2;
		while (argc > 0)
		{
			String cmd = args[args.length - argc];

			if (cmd.equals("-cn"))
			{
				if (argc < 2)
				{
					System.out
							.println("Insufficient arguments, more arguments needed after -cn flag.");
					return false;
				}
				argc--;
				String collectionName = args[args.length - argc];
				if (!configer.setConfByCollection(collectionName))
				{
					System.out
							.println("Collection \""
									+ collectionName
									+ "\" not supported, please specify inputformat and docnomapping class, or contact developer.");
					return false;
				}

			}
			else if (cmd.equals("-ci"))
			{
				if (argc < 2)
				{
					System.out
							.println("Insufficient arguments, more arguments needed after -ci flag.");
					return false;
				}
				argc--;
				String ciName = args[args.length - argc];
				if (!configer.setUserSpecifiedInputFormat(ciName))
				{
					System.out
							.println("class \""
									+ ciName
									+ "\" doesn't exist or not sub-class of FileInputFormat");
					return false;
				}
			}
			else if (cmd.equals("-cm"))
			{
				if (argc < 2)
				{
					System.out
							.println("Insufficient arguments, more arguments needed after -cm flag.");
					return false;
				}
				argc--;
				String cmName = args[args.length - argc];
				
				if (!configer.setUserSpecifiedDocnoMappingClass(cmName))
				{
					System.out
							.println("class \""
									+ cmName
									+ "\" doesn't exist or not implemented DocnoMappingt");
					return false;
				}
				//conf.set("Cloud9.DocnoMappingClass", cmName);
			}
			else if (cmd.equals("-cf"))
			{
				if (argc < 2)
				{
					System.out
							.println("Insufficient arguments, more arguments needed after -cm flag.");
					return false;
				}
				argc--;
				String cfName = args[args.length - argc];
				conf.set("Cloud9.DocnoMappingFile", cfName);
			}
//			else if (cmd.equals("-f"))
//			{
//				if (argc < 2)
//				{
//					System.out
//							.println("Insufficient arguments, more arguments needed after -f flag.");
//					return false;
//				}
//				argc--;
//				filtername = args[args.length - argc];
//				if (filtername.startsWith("-"))
//				{
//					System.out
//							.println("Invalid arguments format, a filtername required after -f flag.");
//					return false;
//				}
//			}
//			else if (cmd.equals("-ss"))
//			{
//				if (argc < 2)
//				{
//					System.out
//							.println("Insufficient arguments, more arguments needed after -ss flag.");
//					return false;
//				}
//				argc--;
//				try
//				{
//					// limitSize = Integer.parseInt(args[args.length - argc]);
//				}
//				catch (NumberFormatException e)
//				{
//					System.out
//							.println("Invalid arguments, Integer is expected after -ss flag.");
//					return false;
//				}
//			}
			else if (cmd.equals("-il"))
			{
				includeInternalLinks = true;
			}
			else if (cmd.equals("-caw"))
			{
				computeAnchorWeights = true;
			}
			else if (cmd.equals("-nm"))
			{
				if (argc < 2)
				{
					System.out
							.println("Insufficient arguments, more arguments needed after -nm flag.");
					return false;
				}
				argc--;
				String nm = args[args.length - argc];
				;
				try
				{
					if (!AnchorTextNormalizer.class.isAssignableFrom(Class
							.forName(nm)))
					{
						System.out
								.println("Invalid arguments; Normalizer class must implement AnchorTextNormalizer interface.");
						return false;
					}
				}
				catch (ClassNotFoundException e)
				{
					System.out
							.println("Invalid arguments; Specified Normalizer class doesn't exist");
					return false;
				}

				normalizer = nm;
			}
			else if (cmd.startsWith("<") && cmd.endsWith(">")
					&& cmd.contains(":"))
			{
				cmd = cmd.substring(1, cmd.length() - 1);
				int pos = cmd.indexOf(":");
				conf.set(cmd.substring(0, pos), cmd.substring(pos));
			}
			else
			{
				System.out.println("Warning: Unresolved argument : " + cmd);
				// moreArgs.insertArg(cmd);
			}
			argc--;
		}
		return true;
	}

}
