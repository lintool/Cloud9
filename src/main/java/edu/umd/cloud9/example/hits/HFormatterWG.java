/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and
 * builds corresponding Hadoop structures for representing the graph. This
 * program constructs a list of nodes with their outgoing links from a encoding
 * of nodes with outgoing links. It expects to
 * Command-line parameters are as follows:
 * </p>
 * 
 * <ul>
 * 
 * <li>[input-path]: input directory</li>
 * <li>[output-path]: output directory</li>
 * <li>[num-mappers]: number of mappers to start</li>
 * <li>[num-reducers]: number of reducers to start</li>
 * <li>[stoplist-path]: path to file containing nodeIDs to ignore</li>
 * </ul>
 * @author Mike McGrath
 * 
 */

public class HFormatterWG extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(HFormatterWG.class);

	/**
	 * @param args
	 */
	private static class HFormatMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, HITSNode> {

		private IntWritable keyOut = new IntWritable();
		HashSet<Integer> stopList = new HashSet<Integer>();

		public void configure(JobConf jc) {
			stopList = readStopList(jc);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {

			HITSNode dataOut = new HITSNode();
			ArrayListOfIntsWritable links = new ArrayListOfIntsWritable();
			dataOut.setType(HITSNode.TYPE_HUB_COMPLETE);

			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);

			if (itr.hasMoreTokens()) {
				int curr = Integer.parseInt(itr.nextToken());
				if (stopList.contains(curr)) {
					return;
				}
				keyOut.set(curr);
				dataOut.setNodeId(keyOut.get());
			}
			while (itr.hasMoreTokens()) {
				// links = new ArrayListOfIntsWritable();
				int curr = Integer.parseInt(itr.nextToken());
				if (!(stopList.contains(curr))) {
					links.add(curr);
				}
			}
			dataOut.setOutlinks(links);
			dataOut.setHRank((float) 0.0);
			System.out.println(">>>" + keyOut.get() + " | " + dataOut.toString());
			output.collect(keyOut, dataOut);
			// emit mentioned mentioner -> mentioned (mentioners) in links
			// emit mentioner mentioned -> mentioner (mentions) outlinks
			// emit mentioned a
			// emit mentioner 1
		}

	}

	private static class HFormatReducer extends MapReduceBase implements
			Reducer<IntWritable, HITSNode, IntWritable, HITSNode> {
		ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
		private HITSNode valIn;
		private HITSNode valOut = new HITSNode();

		public void reduce(IntWritable key, Iterator<HITSNode> values,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {
			adjList.clear();
			// adjList.trimToSize();

			while (values.hasNext()) {
				valIn = values.next();
				ArrayListOfIntsWritable adjListIn = valIn.getOutlinks();
				adjListIn.trimToSize();
				adjList.addUnique(adjListIn.getArray());
				valOut.setNodeId(valIn.getNodeId());
			}
			valOut.setOutlinks(adjList);
			valOut.setType(HITSNode.TYPE_HUB_COMPLETE);
			valOut.setHRank((float) 0.0);

			output.collect(key, valOut);

		}
	}

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [output-path] [num-mappers] [num-reducers] [stoplist-path]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {

		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		String stoplistPath = args[4];

		sLogger.info("Tool: HFormatterWG");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(HFormatterWG.class);
		conf.setJobName("HubFormatter -- WebGraph");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HITSNode.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(HFormatMapper.class);
		conf.setReducerClass(HFormatReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		Path stopList = new Path(stoplistPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		DistributedCache.addCacheFile(stopList.toUri(), conf);
		conf.setStrings("stoplist", stopList.toString());
		JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	private static HashSet<Integer> readStopList(JobConf jc) {
		HashSet<Integer> out = new HashSet<Integer>();
		try {
			//System.out.println(">> " + DistributedCache.getLocalCacheFiles(jc).toString());
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(jc);
			//String[] cacheFiles;
			//cacheFiles = jc.getStrings("stoplist");
			FileReader fr = new FileReader(cacheFiles[0].toString());
			BufferedReader stopReader = new BufferedReader(fr);
			String line;
			while ((line = stopReader.readLine()) != null) {
				out.add(Integer.parseInt(line));
			}
			stopReader.close();
			return out;
		} catch (IOException ioe) {
			System.err.println("IOException reading from distributed cache");
			System.err.println(ioe.toString());
			return out;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HFormatterWG(), args);
		System.exit(res);
	}

}
