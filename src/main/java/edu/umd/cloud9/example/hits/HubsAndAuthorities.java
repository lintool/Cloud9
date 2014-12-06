/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapIF;

/**
 * <p>
 * Main driver program for running the non-schimmy version of Kleinberg's
 * Hubs and Authorities/Hyperlink-Induced Topic Search (HITS) algorithm
 * Command line arguments are:
 * </p>
 * 
 * <ul>
 * <li>[basePath]: the base path</li>
 * <li>[numNodes]: number of nodes in the graph</li>
 * <li>[start]: starting iteration</li>
 * <li>[end]: ending iteration</li>
 * <li>[useCombiner?]: 1 for using combiner, 0 for not</li>
 * <li>[useInMapCombiner?]: 1 for using in-mapper combining, 0 for not</li>
 * <li>[useRange?]: 1 for range partitioning, 0 for not</li>
 * <li>[num Mappers]: number of mappers to use</li>
 * <li>[numReducers]: number of reducers to use. This should remain constant between iterations</li>
 * </ul>
 * 
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p> 
 * 
 * @see HubsAndAuthoritiesSchimmy
 * @author Mike McGrath
 * 
 */
public class HubsAndAuthorities extends Configured implements Tool {

	private static final Logger sLogger = Logger
			.getLogger(HubsAndAuthorities.class);

	/**
	 * @param args
	 */
	private static class HAMapper extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, IntWritable, HITSNode> {
		
		private HITSNode valOut = new HITSNode();
		private ArrayListOfIntsWritable empty = new ArrayListOfIntsWritable();

		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {

			//emit existing h&a vals & structure
			output.collect(key, value);
			
			//auth score for a node X is sum of all hub scores from nodes linking to X
			// so for each outgoing link X1...XN, contribute this node's hub score as part of node X1...XN's auth score
			// ( total auth score will be summed in reducer)
			int typeOut = HITSNode.TYPE_AUTH_MASS;
			ArrayListOfIntsWritable adjList = value.getOutlinks();
			int curr;
			for (int i = 0; i < adjList.size(); i++) {
				curr = adjList.get(i);
				valOut.setType(typeOut);
				valOut.setARank(value.getHRank());
				output.collect(new IntWritable(curr), valOut);
			}
			
			//hub score for a node X is sum of all auth scores from nodes linked from X
			// so for each incoming link X1...XN, contribute this node's auth score as part of node X1...XN's hub score
			// ( total hub score will be summed in reducer)
			typeOut = HITSNode.TYPE_HUB_MASS;
			adjList = value.getInlinks();
			for (int i = 0; i < adjList.size(); i++) {
				curr = adjList.get(i);
				valOut.setType(typeOut);
				valOut.setHRank(value.getARank());
				output.collect(new IntWritable(curr), valOut);
			}
		}

	}

	//wrong!! FIXME1
	// mapper using in-mapper combining
	private static class HAMapperIMC extends MapReduceBase implements
			Mapper<IntWritable, HITSNode, IntWritable, HITSNode> {

		// for buffering rank values
		private static HMapIF rankmapA = new HMapIF();
		private static HMapIF rankmapH = new HMapIF();

		// save a reference to the output collector
		private static OutputCollector<IntWritable, HITSNode> mOutput;

		private static HITSNode valOut = new HITSNode();

		public void configure(JobConf job) {
			rankmapA.clear();
			rankmapH.clear();
		}

		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {

			mOutput = output;
			int typeOut = 0;
			ArrayListOfIntsWritable adjList;
			output.collect(key, value);

			int curr;
			typeOut = 0;
			//emit avals to inlinks as hvals
			adjList = value.getOutlinks();
			for (int i = 0; i < adjList.size(); i++) {
				curr = adjList.get(i);
				if (rankmapA.containsKey(curr)) {
					rankmapA.put(curr, sumLogProbs(rankmapA.get(curr),
							value.getHRank()));
				} else {
					rankmapA.put(curr, value.getHRank());
				}
			}
			//emit hvals to outlinks as avals
			adjList = value.getInlinks();
			for (int i = 0; i < adjList.size(); i++) {
				curr = adjList.get(i);
				if (rankmapH.containsKey(curr)) {
					rankmapH.put(curr, sumLogProbs(rankmapH.get(curr),
							value.getARank()));
				} else {
					rankmapH.put(curr, value.getARank());
				}
			}
		}

		public void close() throws IOException {
			IntWritable n = new IntWritable();
			HITSNode mass = new HITSNode();
			for (MapIF.Entry e : rankmapH.entrySet()) {
				n.set(e.getKey());
				mass.setType(HITSNode.TYPE_HUB_MASS);
				mass.setHRank(e.getValue());
				mass.setNodeId(e.getKey());
				mOutput.collect(n, mass);
			}
			mass = new HITSNode();
			for (MapIF.Entry e : rankmapA.entrySet()) {
				n.set(e.getKey());
				mass.setType(HITSNode.TYPE_AUTH_MASS);
				mass.setARank(e.getValue());
				mass.setNodeId(e.getKey());
				mOutput.collect(n, mass);
			}
		}

	}

	private static class HAReducer extends MapReduceBase implements
			Reducer<IntWritable, HITSNode, IntWritable, HITSNode> {
		private HITSNode valIn;
		private HITSNode valOut = new HITSNode();

		private int jobIter = 0;

		public void configure(JobConf jconf) {
			jobIter = jconf.getInt("jobIter", 0);
		}

		public void reduce(IntWritable key, Iterator<HITSNode> values,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {
			ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();

			float hrank = Float.NEGATIVE_INFINITY;
			float arank = Float.NEGATIVE_INFINITY;

			valOut.setInlinks(adjList);
			valOut.setOutlinks(adjList);

			while (values.hasNext()) {
				valIn = values.next();

				// get type
				int type = valIn.getType();
				adjList.clear();

				if (type == HITSNode.TYPE_NODE_COMPLETE || type == HITSNode.TYPE_NODE_STRUCTURE) {
					//adjList = valIn.getAdjacencyList();
					valOut.setOutlinks(new ArrayListOfIntsWritable(
								valIn.getOutlinks()));
						// System.out.println(key.toString() + " " + "H" + " " +
						// hpayloadOut.toString());
					valOut.setInlinks(new ArrayListOfIntsWritable(
								valIn.getInlinks()));
						// System.out.println(key.toString() + " " + "A" + " " +
						// hpayloadOut.toString());
					} 
				// else add rank to current rank
				else if (type == HITSNode.TYPE_HUB_MASS) {
					hrank = sumLogProbs(hrank, valIn.getHRank());
				} else if (type == HITSNode.TYPE_AUTH_MASS) {
					// arank += rankIn;
					arank = sumLogProbs(arank, valIn.getARank());
				}
				else {
					System.err.println("Unexpected Node Type: " + type);
				}
			}

			// if this is the first run, set rank to 0 for nodes with no inlinks
			// or outlinks
			if (jobIter == 0) {
				if (hrank == Float.NEGATIVE_INFINITY) {
					hrank = 0;
				}
				if (arank == Float.NEGATIVE_INFINITY) {
					arank = 0;
				}
			}
			// build output tuple and write to output
			valOut.setHRank(hrank);
			valOut.setARank(arank);
			valOut.setType(HITSNode.TYPE_NODE_COMPLETE);
			valOut.setNodeId(key.get());

			output.collect(key, valOut);
		}
	}

	private static class Norm1Mapper extends MapReduceBase implements
			Mapper<IntWritable, HITSNode, Text, FloatWritable> {

		FloatWritable rank = new FloatWritable();

		public void map(IntWritable key, HITSNode value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			int type = value.getType();

			if (type == HITSNode.TYPE_NODE_COMPLETE) {
				rank.set(value.getARank() * 2);
				output.collect(new Text("A"), rank);
				rank.set(value.getHRank() * 2);
				output.collect(new Text("H"), rank);
			} else {
				System.err.println("Bad Type: " + type);
			}
		}

	}

	private static class Norm1MapperIMC extends MapReduceBase implements
			Mapper<IntWritable, HITSNode, Text, FloatWritable> {

		private static float hsum = Float.NEGATIVE_INFINITY;
		private static float asum = Float.NEGATIVE_INFINITY;
		private static OutputCollector<Text, FloatWritable> mOutput;

		public void configure(JobConf conf) {
			hsum = Float.NEGATIVE_INFINITY;
			asum = Float.NEGATIVE_INFINITY;
		}

		public void map(IntWritable key, HITSNode value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			mOutput = output;

			int type = value.getType();
			float arank = value.getARank() * 2;
			float hrank = value.getHRank() * 2;// <===FIXME

			if (type == HITSNode.TYPE_AUTH_COMPLETE) {
				asum = sumLogProbs(asum, arank);
			} else if (type == HITSNode.TYPE_HUB_COMPLETE) {
				hsum = sumLogProbs(hsum, hrank);
			} else if (type == HITSNode.TYPE_NODE_COMPLETE || type == HITSNode.TYPE_NODE_MASS){
				asum = sumLogProbs(asum, arank);
				hsum = sumLogProbs(hsum, hrank);
			}
			else {
				System.err.println("Bad Type: " + type);
			}
		}

		public void close() throws IOException {
			if (hsum != Float.NEGATIVE_INFINITY)
				mOutput.collect(new Text("H"), new FloatWritable(hsum));
			if (asum != Float.NEGATIVE_INFINITY)
				mOutput.collect(new Text("A"), new FloatWritable(asum));
		}

	}

	private static class Norm1Combiner extends MapReduceBase implements
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			float sum = Float.NEGATIVE_INFINITY;
			FloatWritable valIn;

			while (values.hasNext()) {
				valIn = values.next();
				sum = sumLogProbs(sum, valIn.get());
			}

			if (sum != Float.NEGATIVE_INFINITY)
				output.collect(key, new FloatWritable(sum));
		}
	}

	private static class Norm1Reducer extends MapReduceBase implements
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			float sum = Float.NEGATIVE_INFINITY;
			FloatWritable valIn;

			while (values.hasNext()) {
				valIn = values.next();
				sum = sumLogProbs(sum, valIn.get());
			}

			sum = sum / 2; // sqrt

			output.collect(key, new FloatWritable(sum));
		}
	}

	private static class Norm2Mapper extends MapReduceBase implements
			Mapper<IntWritable, HITSNode, IntWritable, HITSNode> {

		private HITSNode nodeOut = new HITSNode();

		private float rootSumA;
		private float rootSumH;

		public void configure(JobConf jconf) {
			rootSumA = jconf.getFloat("rootSumA", 0);
			rootSumH = jconf.getFloat("rootSumH", 0);
		}

		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {

			float arank = value.getARank();
			float hrank = value.getHRank();

			hrank = hrank - rootSumH;
			arank = arank - rootSumA;

			nodeOut.setNodeId(key.get());
			nodeOut.setType(HITSNode.TYPE_NODE_COMPLETE);
			nodeOut.setARank(arank);
			nodeOut.setHRank(hrank);
			nodeOut.setInlinks(value.getInlinks());
			nodeOut.setOutlinks(value.getOutlinks());

			output.collect(key, nodeOut);
		}

	}

	private ArrayList<Float> readSums(JobConf jconf, String pathIn)
			throws Exception {
		ArrayList<Float> output = new ArrayList<Float>();
		float rootSumA = -1;
		float rootSumH = -1;
		SequenceFile.Reader reader = null;
		try {
			Configuration cfg = new Configuration();
			FileSystem fs = FileSystem.get(cfg);
			Path sumsIn = new Path(pathIn);
			// FSDataInputStream in = fs.open(sumsIn);

			reader = new SequenceFile.Reader(fs, sumsIn, jconf);
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
					jconf);
			FloatWritable value = (FloatWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), jconf);

			while (reader.next(key, value)) {
				// System.out.printf("%s\t%s\n", key, value);
				if (key.toString().equals("A")) {
					rootSumA = value.get();
				} else if (key.toString().equals("H")) {
					rootSumH = value.get();
				} else {
					System.out.println("PROBLEM");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}

		if (rootSumA == -1 || rootSumH == -1) {
			throw new Exception("error: rootSum == - 1");
		}

		output.add(new Float(rootSumA));
		output.add(new Float(rootSumH));

		return output;
	}

	// adds two log probs
	private static float sumLogProbs(float a, float b) {
		if (a == Float.NEGATIVE_INFINITY)
			return b;

		if (b == Float.NEGATIVE_INFINITY)
			return a;

		if (a < b) {
			return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
		}

		return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
	}

	public int run(String[] args) throws Exception {

		if (args.length != 9) {
			printUsage();
			return -1;
		}

		String basePath = args[0];
		int n = Integer.parseInt(args[1]);
		int s = Integer.parseInt(args[2]);
		int e = Integer.parseInt(args[3]);
		boolean useCombiner = Integer.parseInt(args[4]) != 0;
		boolean useInmapCombiner = Integer.parseInt(args[5]) != 0;
		boolean useRange = Integer.parseInt(args[6]) != 0;
		int mapTasks = Integer.parseInt(args[7]);
		int reduceTasks = Integer.parseInt(args[8]);

		sLogger.info("Tool name: HubsAndAuthorities");
		sLogger.info(" - base dir: " + basePath);
		sLogger.info(" - node count: " + n);
		sLogger.info(" - start iteration: " + s);
		sLogger.info(" - end iteration: " + e);
		sLogger.info(" - useCombiner: " + useCombiner);
		sLogger.info(" - useInmapCombiner: " + useInmapCombiner);
		sLogger.info(" - useRange: " + useRange);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		for (int i = s; i < e; i++) {
			iterateHA(basePath, i, i + 1, n, useCombiner, useInmapCombiner,
					useRange, mapTasks, reduceTasks);
		}

		return 0;
	}

	public HubsAndAuthorities() {
	}

	private NumberFormat sFormat = new DecimalFormat("0000");

	private void iterateHA(String path, int i, int j, int n,
			boolean useCombiner, boolean useInmapCombiner, boolean useRange,
			int mapTasks, int reduceTasks) throws IOException {
		HACalc(path, i, j, n, useCombiner, useInmapCombiner, useRange,
				mapTasks, reduceTasks);
		Norm(path, i, j, n, useCombiner, useInmapCombiner, useRange, mapTasks,
				reduceTasks);
	}

	private static int printUsage() {
		System.out
				.println("usage: [base-path] [num-nodes] [start] [end] [useCombiner?] [useInMapCombiner?] [useRange?] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int HACalc(String path, int iter, int jter, int nodeCount,
			boolean useCombiner, boolean useInmapCombiner, boolean useRange,
			int mapTasks, int reduceTasks) throws IOException {

		String inputPath = path + "/iter" + sFormat.format(iter);
		String outputPath = path + "/iter" + sFormat.format(jter) + "t";

		sLogger.info("Tool: HubsAndAuthorities");
		sLogger.info(" - iteration: " + iter);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(HubsAndAuthorities.class);
		conf.setJobName("Iter" + iter + "HubsAndAuthorities");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HITSNode.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		if (useInmapCombiner == true) {
			conf.setMapperClass(HAMapperIMC.class);
		} else {
			conf.setMapperClass(HAMapper.class);
		}

		if (useRange == true) {
			conf.setPartitionerClass(RangePartitioner.class);
		}
		conf.setReducerClass(HAReducer.class);

		conf.setInt("jobIter", iter);
		conf.setInt("NodeCount", nodeCount);

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

	public int Norm(String path, int iter, int jter, int nodeCount,
			boolean useCombiner, boolean useInmapCombiner, boolean useRange,
			int mapTasks, int reduceTasks) throws IOException {

		String inputPath = path + "/iter" + sFormat.format(jter) + "t";
		String outputPath = path + "/iter" + sFormat.format(jter);
		String tempPath = path + "/sqrt";

		sLogger.info("Tool: Normalizer");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - iteration: " + iter);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(HubsAndAuthorities.class);
		conf.setJobName("Iter" + iter + "NormalizerStep1");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(tempPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		if (useInmapCombiner == true) {
			conf.setMapperClass(Norm1MapperIMC.class);
		} else {
			conf.setMapperClass(Norm1Mapper.class);
		}
		if (useCombiner == true) {
			conf.setCombinerClass(Norm1Combiner.class);
		}
		conf.setReducerClass(Norm1Reducer.class);

		JobConf conf2 = new JobConf(HubsAndAuthorities.class);
		conf2.setJobName("Iter" + iter + "NormalizerStep2");
		conf2.setInt("NodeCount", nodeCount);

		conf2.setNumMapTasks(mapTasks);
		conf2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf2, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf2, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf2, false);

		conf2.setInputFormat(SequenceFileInputFormat.class);
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(HITSNode.class);
		conf2.setOutputFormat(SequenceFileOutputFormat.class);

		conf2.setMapperClass(Norm2Mapper.class);
		if (useRange == true) {
			conf2.setPartitionerClass(RangePartitioner.class);
		}
		conf2.setReducerClass(IdentityReducer.class);

		// Delete the output directory if it exists already
		Path tempDir = new Path(tempPath);
		FileSystem.get(conf).delete(tempDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		Path outputDir = new Path(outputPath);

		// read sums
		ArrayList<Float> sums = new ArrayList<Float>();
		try {
			sums = readSums(conf2, tempPath + "/part-00000");
		} catch (Exception e) {
			System.err.println("Failed to read in Sums");
			System.exit(1);
		}

		// conf2.set("rootSumA", sums.get(0).toString());
		conf2.setFloat("rootSumA", sums.get(0));
		// conf2.set("rootSumH", sums.get(1).toString());
		conf2.setFloat("rootSumH", sums.get(1));

		FileSystem.get(conf2).delete(outputDir, true);

		startTime = System.currentTimeMillis();
		JobClient.runJob(conf2);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HubsAndAuthorities(),
				args);
		System.exit(res);
	}

}
