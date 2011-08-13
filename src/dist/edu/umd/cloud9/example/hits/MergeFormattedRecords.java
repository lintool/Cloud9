package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * 
 * <p>
 * Driver program to merges the output of HFormatterWG and AFormatterWG into a 
 * single set of  of output files. It takes five command line arguments:
 * </p>
 * 
 * <ul>
 * <li>[hub-input-path]: input directory containing output of HFormatterWG</li>
 * <li>[auth-input-path]: input directory containing output of AFormatterWG</li>
 * <li>[output-path]: output directory</li>
 * <li>[num-mappers]: number of mappers to use (may be overridden by Hadoop)</li>
 * <li>[num-reducers]: number of reducers to use, also the number of output files</li>
 * </ul>
 * 
 * @see HFormatterWG
 * @see AFormatterWG
 * @author Mike McGrath
 *
 */

public class MergeFormattedRecords extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(MergeFormattedRecords.class);

	private static class MergeReducer extends MapReduceBase implements
			Reducer<IntWritable, HITSNode, IntWritable, HITSNode> {
		public void reduce(IntWritable key, Iterator<HITSNode> values,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {
			ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();

			//construct new HITSNode
			HITSNode nodeOut = new HITSNode();
			
			nodeOut.setType(HITSNode.TYPE_NODE_COMPLETE);
			nodeOut.setARank(0);
			nodeOut.setInlinks(new ArrayListOfIntsWritable());
			nodeOut.setHRank(0);
			nodeOut.setOutlinks(new ArrayListOfIntsWritable());
			nodeOut.setNodeId(key.get());
			
			while (values.hasNext()) {
				HITSNode nodeIn = values.next();
				if (nodeIn.getType() == HITSNode.TYPE_HUB_COMPLETE)
				{
					nodeOut.setHRank(nodeIn.getHRank());
					nodeOut.setOutlinks(new ArrayListOfIntsWritable(nodeIn.getOutlinks()));
				}
				if (nodeIn.getType() == HITSNode.TYPE_AUTH_COMPLETE)
				{
					nodeOut.setARank(nodeIn.getARank());
					nodeOut.setInlinks(new ArrayListOfIntsWritable(nodeIn.getInlinks()));
				}
			}
			output.collect(key, nodeOut);
		}
	}

	private static int printUsage() {
		System.out
				.println("usage: [hub-input-path] [auth-input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String hInputPath = args[0];
		String aInputPath = args[1];
		String outputPath = args[2];

		int mapTasks = Integer.parseInt(args[3]);
		int reduceTasks = Integer.parseInt(args[4]);

		sLogger.info("Tool: MergeFormattedRecords");
		sLogger.info(" - input paths: " + hInputPath + " " + aInputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(MergeFormattedRecords.class);
		conf.setJobName("HAMergeFormattedRecords");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(hInputPath));
		FileInputFormat.addInputPath(conf, new Path(aInputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HITSNode.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(MergeReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MergeFormattedRecords(), args);
		System.exit(res);
	}

}
