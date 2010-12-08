/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
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

import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.example.hits.HITSNode;
import edu.umd.cloud9.example.hits.RangePartitioner;

/**
 * @author michaelmcgrath
 *
 */
public class Normalize extends Configured implements Tool {
	
	private static final Logger sLogger = Logger.getLogger(Normalize.class);
	
	/**
	 * @param args
	 */
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
		
		public void map(IntWritable key, HITSNode value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
			
			mOutput = output;
			
			int type = value.getType();
			float rank = value.getHARank() * 2;
			
			//System.out.println(key.toString() + " " + valOut.toString());
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
		
		private ArrayListWritable<Text> emptyList = new ArrayListWritable<Text>();
		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException
		{
			ArrayListWritable<Text> adjList = new ArrayListWritable<Text>();
			//ArrayListWritable<Text> adjListOut = new ArrayListWritable<Text>();
			float sum = Float.NEGATIVE_INFINITY;
			FloatWritable valIn;
			//DoubleWritable rankIn;

			while (values.hasNext())
			{
				valIn = (FloatWritable) values.next();
				sum = sumLogProbs(sum, valIn.get());
			}
			
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
			//DoubleWritable rankIn;

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
				//System.out.println(rank);
				rank = rank - rootSumH;
			}
			else if (type.equals("A"))
			{
				//System.out.println(rank);
				rank = rank - rootSumA;
			}
			else
			{
				try {
					throw new Exception("Invalid Rank Type");
				} catch (Exception e) {
					// TODO Auto-generated catch block
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
	
	
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
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
				// TODO Auto-generated catch block
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
	
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 6) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String tempPath = "/tmp/sqrt";
		
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);
		int nodeCount = Integer.parseInt(args[4]);
		int iter = Integer.parseInt(args[5]);

		sLogger.info("Tool: Normalizer");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
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

		conf.setMapperClass(Norm1MapperIMC.class);
		conf.setCombinerClass(Norm1Combiner.class);
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
		conf2.setPartitionerClass(RangePartitioner.class);
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
		//need to pass the A + H sum values around as strings b/c hadoop cannot send doubles thru the job conf
		//...dont want to risk casting to float ...
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
		int res = ToolRunner.run(new Configuration(), new Normalize(), args);
		System.exit(res);
	}

}
