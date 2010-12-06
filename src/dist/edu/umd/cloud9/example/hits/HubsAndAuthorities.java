/**
 * 
 */
package ivory.hits;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sun.awt.SunHints.Value;

import java.util.HashMap;
import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.HMapIF;
import ivory.hits.HITSNode;
import ivory.pagerank.RangePartitioner;
import edu.umd.cloud9.util.MapIF;
import edu.umd.cloud9.util.HMapIF;

/**
 * @author michaelmcgrath
 *
 */
public class HubsAndAuthorities extends Configured implements Tool {
	
	private static final Logger sLogger = Logger.getLogger(HubsAndAuthorities.class);
	
	/**
	 * @param args
	 */
	private static class HAMapper extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, IntWritable, HITSNode>
	{
		//private Tuple valIn = MAP_SCHEMA.instantiate();
		private HITSNode valOut = new HITSNode();
		private ArrayListOfIntsWritable empty = new ArrayListOfIntsWritable();
	
		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter) throws IOException {
		
			int type = value.getType();
			int typeOut = 0;
			ArrayListOfIntsWritable adjList = value.getAdjacencyList();
			if (type == HITSNode.TYPE_AUTH_COMPLETE)
			{
				typeOut = HITSNode.TYPE_AUTH_STRUCTURE;
			}
			if (type == HITSNode.TYPE_HUB_COMPLETE)
			{
				typeOut = HITSNode.TYPE_HUB_STRUCTURE;
			}
			valOut.setType(typeOut);
			valOut.setAdjacencyList(adjList);
			
			output.collect(key, valOut);
			
			typeOut = 0;
			
			//check type using new types
			if (value.getType() == HITSNode.TYPE_AUTH_COMPLETE)
			{
				typeOut = HITSNode.TYPE_AUTH_MASS;
			}
			else if (value.getType() == HITSNode.TYPE_HUB_COMPLETE)
			{
				typeOut = HITSNode.TYPE_HUB_MASS;
			}
			else
			{
				System.err.print("Unknown node type: " + typeOut);
			}
			
			valOut.setType(typeOut);
			valOut.setHARank(value.getHARank());
			//valOut.setAdjacencyList(empty);
			valOut.setNodeId(value.getNodeId());
			
			output.collect(key, valOut);
			
			//Iterator itr = adjList.iterator();
			int curr;
			typeOut = 0;
			
			//check type using new types
			if (value.getType() == HITSNode.TYPE_AUTH_COMPLETE || 
				value.getType() == HITSNode.TYPE_AUTH_MASS)
			{
				typeOut = HITSNode.TYPE_HUB_MASS;
			}
			else if (value.getType() == HITSNode.TYPE_HUB_COMPLETE || 
					value.getType() == HITSNode.TYPE_HUB_MASS)
			{
				typeOut = HITSNode.TYPE_AUTH_MASS;
			}
			else
			{
				System.err.print("Unknown node type: " + typeOut);
			}
			
			for (int i = 0; i < adjList.size(); i++)
			{
				curr = adjList.get(i);
				valOut.setType(typeOut);
				valOut.setHARank(value.getHARank());
				valOut.setAdjacencyList(empty);
				
				output.collect(new IntWritable(curr), valOut);
			}	
		}
		
	}
	
	//mapper using in-mapper combining
	private static class HAMapperIMC extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, IntWritable, HITSNode>
	{
		
		//for buffering rank values
		private static HMapIF rankmapA = new HMapIF();
		private static HMapIF rankmapH = new HMapIF();
		
		// save a reference to the output collector
		private static OutputCollector<IntWritable, HITSNode> mOutput;
		
		private static HITSNode valOut = new HITSNode();
		//private static ArrayListOfIntsWritable empty = new ArrayListOfIntsWritable();
	
		public void configure(JobConf job)
		{
			rankmapA.clear();
			rankmapH.clear();
		}
		
		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter) throws IOException {
			
			mOutput = output;
			
			int type = value.getType();
			int typeOut = 0;
			ArrayListOfIntsWritable adjList = value.getAdjacencyList();
			if (type == HITSNode.TYPE_AUTH_COMPLETE)
			{
				typeOut = HITSNode.TYPE_AUTH_STRUCTURE;
			}
			if (type == HITSNode.TYPE_HUB_COMPLETE)
			{
				typeOut = HITSNode.TYPE_HUB_STRUCTURE;
			}
			valOut.setType(typeOut);
			valOut.setAdjacencyList(adjList);
			valOut.setNodeId(key.get());
			
			//System.out.println("[key: " + key.toString() + " ] <value: " + valOut.toString() + " >");
			output.collect(key, valOut);
			
			typeOut = 0;
			
			//check type using new types
			if (value.getType() == HITSNode.TYPE_AUTH_COMPLETE)
			{
				if (rankmapA.containsKey(key.get()))
				{
					rankmapA.put(key.get(), sumLogProbs(rankmapA.get(key.get()), value.getHARank()));
				}
				else
				{
					rankmapA.put(key.get(), value.getHARank());
					//rankmapA.put(key.get(), sumLogProbs(rankmapA.get(key.get()), 0));
				}
			}
			
			else if (value.getType() == HITSNode.TYPE_HUB_COMPLETE)
			{
				if (rankmapH.containsKey(key.get()))
				{
					rankmapH.put(key.get(), sumLogProbs(rankmapH.get(key.get()), value.getHARank()));
				}
				else
				{
					rankmapH.put(key.get(), value.getHARank());
					//rankmapH.put(key.get(), sumLogProbs(rankmapH.get(key.get()), 0));
				}
			}
			else
			{
				System.err.print("Unknown node type: " + typeOut);
			}
			
			int curr;
			typeOut = 0;
			
			for (int i = 0; i < adjList.size(); i++)
			{
				curr = adjList.get(i);
				//System.out.println("[key: " + key.toString() + "] [curr: " + curr + "]");
				if (value.getType() == HITSNode.TYPE_AUTH_COMPLETE)
				{
					if (rankmapH.containsKey(curr))
					{
						rankmapH.put(curr, sumLogProbs(rankmapH.get(curr), value.getHARank()));
					}
					else
					{
						rankmapH.put(curr, value.getHARank());
					}
				}
				else if (value.getType() == HITSNode.TYPE_HUB_COMPLETE)
				{
					if (rankmapA.containsKey(curr))
					{
						rankmapA.put(curr, sumLogProbs(rankmapA.get(curr), value.getHARank()));
					}
					else
					{
						rankmapA.put(curr, value.getHARank());
					}
				}
				else
				{
					System.err.println("Unknown node type: " + value.getType());
				}
			}	
		}
		
		public void close() throws IOException
		{
			IntWritable n = new IntWritable();
			HITSNode mass = new HITSNode();
			for (MapIF.Entry e : rankmapH.entrySet())
			{
				n.set(e.getKey());
				mass.setType(HITSNode.TYPE_HUB_MASS);
				mass.setHARank(e.getValue());
				mass.setNodeId(e.getKey());
				//System.out.println(e.getKey() + " " + e.getValue());
				mOutput.collect(n, mass);
			}
			for (MapIF.Entry e : rankmapA.entrySet())
			{
				n.set(e.getKey());
				mass.setType(HITSNode.TYPE_AUTH_MASS);
				mass.setHARank(e.getValue());
				mass.setNodeId(e.getKey());
				//System.out.println(e.getKey() + " " + e.getValue());
				mOutput.collect(n, mass);
			}
		}
		
	}
	
	
	private static class HAReducer extends MapReduceBase implements
	Reducer<IntWritable, HITSNode, IntWritable, HITSNode>
	{
		private HITSNode valIn;
		private HITSNode hvalOut = new HITSNode();
		private HITSNode avalOut = new HITSNode();
		
		private int jobIter = 0;
		
		public void configure(JobConf jconf)
		{
			jobIter = jconf.getInt("jobIter", 0);
		} 
		
		public void reduce(IntWritable key, Iterator<HITSNode> values, OutputCollector<IntWritable, HITSNode> output,
				Reporter reporter) throws IOException
		{
			ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
			
			float hrank = Float.NEGATIVE_INFINITY;
			float arank = Float.NEGATIVE_INFINITY;
			
			hvalOut.setAdjacencyList(adjList);
			avalOut.setAdjacencyList(adjList);
			
			while (values.hasNext())
			{
				valIn = values.next();
				
				//get type
				int type = valIn.getType();
				adjList.clear();
				
				if (type == HITSNode.TYPE_AUTH_STRUCTURE || 
					type == HITSNode.TYPE_HUB_STRUCTURE)
				{
					adjList = valIn.getAdjacencyList();
					if (type == HITSNode.TYPE_HUB_STRUCTURE)
					{
						//System.out.println("H> " + key.toString() + " " + type + " " + adjList.toString());
						hvalOut.setAdjacencyList(new ArrayListOfIntsWritable(adjList));
						//System.out.println(key.toString() + " " + "H" + " " + hpayloadOut.toString());
					}
					else if (type == HITSNode.TYPE_AUTH_STRUCTURE)
					{
						//System.out.println("A> " + key.toString() + " " + type + " " + adjList.toString());
						avalOut.setAdjacencyList(new ArrayListOfIntsWritable(adjList));
						//System.out.println(key.toString() + " " + "A" + " " + hpayloadOut.toString());
					}
					else
					{
						System.err.println("Unknown Node Type: " + type);
					}
				}
				//else add rank to current rank
				else
				{
					float rankIn = valIn.getHARank();
					if (type == HITSNode.TYPE_HUB_MASS)
					{
						//hrank += rankIn;
						hrank = sumLogProbs(hrank, rankIn);
					}
					else if (type == HITSNode.TYPE_AUTH_MASS)
					{
						//arank += rankIn;
						arank = sumLogProbs(arank, rankIn);
					}
				}
			}
			//System.out.println(key.toString() + " " + "H" + " " + hpayloadOut.toString());
			
			//if this is the first run, set rank to 0 for nodes with no inlinks or outlinks
			if (jobIter == 0)
			{
				if (hrank == Float.NEGATIVE_INFINITY)
				{
					hrank = 0;
				}
				if (arank == Float.NEGATIVE_INFINITY)
				{
					arank = 0;
				}
			}
			//build output tuple and write to output
			hvalOut.setHARank(hrank);
			avalOut.setHARank(arank);
			hvalOut.setType(HITSNode.TYPE_HUB_COMPLETE);
			avalOut.setType(HITSNode.TYPE_AUTH_COMPLETE);
			hvalOut.setNodeId(key.get());
			avalOut.setNodeId(key.get());
			
			output.collect(key, hvalOut);
			output.collect(key, avalOut);
		}
	}
	
	private static class Norm1Mapper extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, Text, FloatWritable>
	{

		FloatWritable rank = new FloatWritable();
		public void map(IntWritable key, HITSNode value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		
			int type = value.getType();
			rank.set(value.getHARank() * 2);
			
			//System.out.println(key.toString() + " " + valOut.toString());
			String textType = "?";
			if (type == HITSNode.TYPE_AUTH_COMPLETE)
			{
				textType = "A";
			}
			else if (type == HITSNode.TYPE_HUB_COMPLETE)
			{
				textType = "H";
			}
			else
			{
				System.err.println("Bad Type: " + type);
			}
			output.collect(new Text(textType), rank);
		}
		
	}
	
	private static class Norm1MapperIMC extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, Text, FloatWritable>
	{

		private static float hsum = Float.NEGATIVE_INFINITY;
		private static float asum = Float.NEGATIVE_INFINITY;
		private static OutputCollector<Text, FloatWritable> mOutput;
		
		public void configure(JobConf conf)
		{
			hsum = Float.NEGATIVE_INFINITY;
			asum = Float.NEGATIVE_INFINITY;
		}
		
		public void map(IntWritable key, HITSNode value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
			
			mOutput = output;
			
			int type = value.getType();
			float rank = value.getHARank() * 2;
			
			if (type == HITSNode.TYPE_AUTH_COMPLETE)
			{
				asum = sumLogProbs(asum, rank);
			}
			else if (type == HITSNode.TYPE_HUB_COMPLETE)
			{
				hsum = sumLogProbs(hsum, rank);
			}
			else
			{
				System.err.println("Bad Type: " + type);
			}
		}
		
		public void close() throws IOException
		{
			if (hsum != Float.NEGATIVE_INFINITY)
				mOutput.collect(new Text("H"), new FloatWritable(hsum));
			if (asum != Float.NEGATIVE_INFINITY)
				mOutput.collect(new Text("A"), new FloatWritable(asum));
		}
		
	}
	
	private static class Norm1Combiner extends MapReduceBase implements
	Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		
		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException
		{
			float sum = Float.NEGATIVE_INFINITY;
			FloatWritable valIn;

			while (values.hasNext())
			{
				valIn = values.next();
				sum = sumLogProbs(sum, valIn.get());
			}
			
			if (sum != Float.NEGATIVE_INFINITY)
				output.collect(key, new FloatWritable(sum));
		}
	}
	
	private static class Norm1Reducer extends MapReduceBase implements
	Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		
		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException
		{
			float sum = Float.NEGATIVE_INFINITY;
			FloatWritable valIn;

			while (values.hasNext())
			{
				valIn = values.next();
				sum = sumLogProbs(sum, valIn.get());
			}
			
			sum = sum / 2; //sqrt
			
			output.collect(key, new FloatWritable(sum));
		}
	}
	
	private static class Norm2Mapper extends MapReduceBase implements
	Mapper<IntWritable, HITSNode, IntWritable, HITSNode>
	{

		private HITSNode nodeOut = new HITSNode();
		
		private float rootSumA;
		private float rootSumH;
		
		public void configure(JobConf jconf)
		{
			rootSumA = jconf.getFloat("rootSumA", 0);
			rootSumH = jconf.getFloat("rootSumH", 0);
		} 

	
		public void map(IntWritable key, HITSNode value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter) throws IOException {
		
			//System.out.println("H: " + rootSumH);
			//System.out.println("A: " + rootSumA);
			int typeI = value.getType();
			String type = "?";
			if (typeI == HITSNode.TYPE_HUB_COMPLETE)
			{
				type = "H";
			}
			else if (typeI == HITSNode.TYPE_AUTH_COMPLETE)
			{
				type = "A";
			}
			
			float rank = value.getHARank();
			
			if (type.equals("H"))
			{
				rank = rank - rootSumH;
			}
			else if (type.equals("A"))
			{
				rank = rank - rootSumA;
			}
			else
			{
				try {
					throw new Exception("Invalid Rank Type");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			nodeOut.setNodeId(key.get());
			nodeOut.setType(typeI);
			nodeOut.setHARank(rank);
			nodeOut.setAdjacencyList(value.getAdjacencyList());
			//System.out.println(tupleOut.toString());

			//System.out.println(key.toString() + " " + valOut.toString());
			output.collect(key, nodeOut);
		}
		
	}
	
	private ArrayList<Float> readSums(JobConf jconf, String pathIn) throws Exception
	{
		ArrayList<Float> output = new ArrayList<Float>();
		float rootSumA = -1;
		float rootSumH = -1;
		SequenceFile.Reader reader = null;
		try
		{
			Configuration cfg = new Configuration(); 
			FileSystem fs = FileSystem.get(cfg); 
			Path sumsIn = new Path(pathIn); 
			//FSDataInputStream in = fs.open(sumsIn); 
			
			reader = new SequenceFile.Reader(fs, sumsIn, jconf);
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), jconf);
			FloatWritable value = (FloatWritable) ReflectionUtils.newInstance(reader.getValueClass(), jconf);

			while (reader.next(key, value)) {
			        //System.out.printf("%s\t%s\n", key, value);
			        if (key.toString().equals("A"))
			        {
			        	rootSumA = value.get();
			        }
			        else if (key.toString().equals("H"))
			        {
			        	rootSumH = value.get();
			        }
			        else
			        {
			        	System.out.println("PROBLEM");
			        }
			      }
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
			  IOUtils.closeStream(reader);
			}
			
			if (rootSumA == -1 || rootSumH == -1)
			{
				throw new Exception("error: rootSum == - 1");
			}
			
			output.add(new Float(rootSumA));
			output.add(new Float (rootSumH));
			
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
			iterateHA(basePath, i, i + 1, n, useCombiner, useInmapCombiner, useRange, mapTasks, reduceTasks);
		}

		return 0;
	}
	
	public HubsAndAuthorities() {
	}
	
	private NumberFormat sFormat = new DecimalFormat("0000");
	
	private void iterateHA(String path, int i, int j, int n, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange, int mapTasks, int reduceTasks) throws IOException 
	{
		HACalc(path, i, j, n, useCombiner, useInmapCombiner, useRange, mapTasks, reduceTasks);
		Norm(path, i, j, n, useCombiner, useInmapCombiner, useRange, mapTasks, reduceTasks);
	}

	private static int printUsage() {
		System.out.println("usage: [base-path] [num-nodes] [start] [end] [useCombiner?] [useInMapCombiner?] [useRange?] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	
	public int HACalc(String path, int iter, int jter, int nodeCount, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange, int mapTasks, int reduceTasks) throws IOException {
		
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

		if (useInmapCombiner == true)
		{
			conf.setMapperClass(HAMapperIMC.class);
		}
		else
		{
			conf.setMapperClass(HAMapper.class);
		}
		
		if (useRange == true)
		{
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
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		return 0;
	}
	
	public int Norm(String path, int iter, int jter, int nodeCount, boolean useCombiner, 
			boolean useInmapCombiner, boolean useRange, int mapTasks, int reduceTasks) throws IOException {
		
		//FIXME
		String inputPath = path + "/iter" + sFormat.format(jter) + "t"; 
		String outputPath = path + "/iter" + sFormat.format(jter); 
		String tempPath = path + "/sqrt";
				
		sLogger.info("Tool: Normalizer");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - iteration: " + iter);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(Normalize.class);
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

		if (useInmapCombiner == true)
		{
			conf.setMapperClass(Norm1MapperIMC.class);
		}
		else
		{
			conf.setMapperClass(Norm1Mapper.class);
		}
		if (useCombiner == true)
		{
			conf.setCombinerClass(Norm1Combiner.class);
		}
		conf.setReducerClass(Norm1Reducer.class);
		
		JobConf conf2 = new JobConf(Normalize.class);
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
		if (useRange == true)
		{
			conf2.setPartitionerClass(RangePartitioner.class);
		}
		conf2.setReducerClass(IdentityReducer.class);

		// Delete the output directory if it exists already
		Path tempDir = new Path(tempPath);
		FileSystem.get(conf).delete(tempDir, true);
		
		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		Path outputDir = new Path(outputPath);
		
		//read sums
		ArrayList<Float> sums = new ArrayList<Float>();
		try {
			sums = readSums(conf2, tempPath + "/part-00000");
		}
		catch (Exception e)
		{
			System.err.println("Failed to read in Sums");
			System.exit(1);
		}
		
		//conf2.set("rootSumA", sums.get(0).toString());
		conf2.setFloat("rootSumA", sums.get(0));
		//conf2.set("rootSumH", sums.get(1).toString());
		conf2.setFloat("rootSumH", sums.get(1));
		
		FileSystem.get(conf2).delete(outputDir, true);
		
		startTime = System.currentTimeMillis();
		JobClient.runJob(conf2);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HubsAndAuthorities(), args);
		System.exit(res);
	}

}
