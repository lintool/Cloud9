/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.map.HMapIV;
import edu.umd.cloud9.util.map.MapIV;

/**
 * @author michaelmcgrath
 * 
 */
public class InlinkCounter extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(InlinkCounter.class);

	/**
	 * @param args
	 */
	private static class AFormatMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, LongWritable> {
		private LongWritable valOut = new LongWritable(1);
		private IntWritable keyOut = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, LongWritable> output,
				Reporter reporter) throws IOException {

			ArrayListOfIntsWritable links = new ArrayListOfIntsWritable();
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			if (itr.hasMoreTokens()) {
				itr.nextToken();
			}
			while (itr.hasMoreTokens()) {
				keyOut.set(Integer.parseInt(itr.nextToken()));
				output.collect(keyOut, valOut);
			}
			// emit mentioned mentioner -> mentioned (mentioners) in links
			// emit mentioner mentioned -> mentioner (mentions) outlinks
			// emit mentioned a
			// emit mentioner 1

		}

	}

	private static class AFormatMapperIMC extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, HITSNode> {
		private HITSNode valOut = new HITSNode();
		private IntWritable keyOut = new IntWritable();
		private static OutputCollector<IntWritable, HITSNode> mOutput;
		private static HMapIV<ArrayListOfIntsWritable> adjLists = new HMapIV<ArrayListOfIntsWritable>();

		public void configure(JobConf jc) {
			adjLists.clear();
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {

			mOutput = output;

			ArrayListOfIntsWritable links = new ArrayListOfIntsWritable();
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			if (itr.hasMoreTokens()) {
				links.add(Integer.parseInt(itr.nextToken()));
				// add to HMap here
			}
			while (itr.hasMoreTokens()) {
				int curr = Integer.parseInt(itr.nextToken());
				if (adjLists.containsKey(curr)) {
					ArrayListOfIntsWritable list = adjLists.get(curr);
					list.trimToSize();
					links.trimToSize();
					//FIXME
					//list.addAll(links.getArray());
					adjLists.put(curr, list);
				} else {
					links.trimToSize();
					adjLists.put(curr, links);
				}
			}
		}

		public void close() throws IOException {
			for (MapIV.Entry<ArrayListOfIntsWritable> e : adjLists.entrySet()) {
				keyOut.set(e.getKey());
				valOut.setNodeId(e.getKey());
				valOut.setARank((float) 0.0);
				valOut.setHRank((float) 0.0);
				valOut.setType(HITSNode.TYPE_AUTH_COMPLETE);
				//FIXME
				//valOut.setAdjacencyList(e.getValue());
				mOutput.collect(keyOut, valOut);
			}
		}

	}

	private static class AFormatCombiner extends MapReduceBase implements
			Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		private LongWritable valIn;
		private LongWritable valOut = new LongWritable();
		ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();

		public void reduce(IntWritable key, Iterator<LongWritable> values,
				OutputCollector<IntWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			// ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
			long sum = 0;
			// System.out.println(key.toString());
			// System.out.println(adjList.toString());
			while (values.hasNext()) {
				sum += values.next().get();
			}
			valOut.set(sum);
			output.collect(key, valOut);
		}
	}

	private static class AFormatReducer extends MapReduceBase implements
			Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
		private LongWritable valIn;
		private LongWritable valOut = new LongWritable();
		ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();

		public void reduce(IntWritable key, Iterator<LongWritable> values,
				OutputCollector<IntWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			// ArrayListOfIntsWritable adjList = new ArrayListOfIntsWritable();
			long sum = 0;
			// System.out.println(key.toString());
			// System.out.println(adjList.toString());
			while (values.hasNext()) {
				sum += values.next().get();
			}

			if (sum > 100000) {
				valOut.set(sum);
				output.collect(key, valOut);
			}

		}
	}

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {

		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: Counter");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(InlinkCounter.class);
		conf.setJobName("InlinkCounter -- Web Graph");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		// conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LongWritable.class);
		// conf.setOutputFormat(SequenceFileOutputFormat.class);

		// InputSampler.Sampler<IntWritable, Text> sampler = new
		// InputSampler.RandomSampler<IntWritable, Text>(0.1, 10, 10);
		// InputSampler.writePartitionFile(conf, sampler);
		// conf.setPartitionerClass(TotalOrderPartitioner.class);
		conf.setMapperClass(AFormatMapper.class);
		conf.setCombinerClass(AFormatCombiner.class);
		conf.setReducerClass(AFormatReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		sLogger.info("Starting job");
		JobClient.runJob(conf);
		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new InlinkCounter(), args);
		System.exit(res);
	}

}
