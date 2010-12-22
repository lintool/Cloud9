/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sun.awt.SunHints.Value;

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;

/**
 * @author michaelmcgrath
 * 
 */
public class SortByAuthRank extends Configured implements Tool {

	private static final Schema MAP_SCHEMA = new Schema();
	private static final Schema VAL_SCHEMA = new Schema();
	private static final Schema PAYLOAD_SCHEMA = new Schema();

	static {
		MAP_SCHEMA.addField("link", String.class);
		MAP_SCHEMA.addField("rank", Double.class);
		VAL_SCHEMA.addField("rankType", String.class);
		VAL_SCHEMA.addField("payload", Tuple.class);
		PAYLOAD_SCHEMA.addField("rank", DoubleWritable.class);
		PAYLOAD_SCHEMA.addField("adjList", ArrayListWritable.class);
	}

	private static final Logger sLogger = Logger
			.getLogger(SortByAuthRank.class);

	/**
	 * @param args
	 */
	private static class ASortMapper extends MapReduceBase implements
			Mapper<Text, Tuple, DoubleWritable, Text> {
		// private Tuple valIn = MAP_SCHEMA.instantiate();
		private Tuple payloadIn = PAYLOAD_SCHEMA.instantiate();

		private ArrayListWritable<Text> empty = new ArrayListWritable<Text>();
		private final static DoubleWritable dummy = new DoubleWritable(-1.0);

		public void map(Text key, Tuple value,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {

			String type = value.getSymbol("rankType");
			payloadIn = (Tuple) value.get("payload");
			DoubleWritable outputKey = (DoubleWritable) payloadIn.get("rank");

			// payloadIn
			if (type.equals("H")) {
				double out = outputKey.get(); // * 10;
				// DecimalFormat rounded = new DecimalFormat("#.##########");

				// out = Double.valueOf(rounded.format(out));
				out = Math.abs(Math.ceil(Math.log10(out)));
				outputKey.set(out);
				output.collect(outputKey, key);
			}
		}

	}

	private static class ASortReducer extends MapReduceBase implements
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		// private Tuple valIn = VAL_SCHEMA.instantiate();
		// private Tuple payloadIn = PAYLOAD_SCHEMA.instantiate();

		public void reduce(DoubleWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {
			Integer sum = new Integer(0);
			while (values.hasNext()) {
				Text curr = values.next();
				/*
				 * if (key.get() >= 0.1) { output.collect(key, curr); }
				 */
				sum++;
			}
			output.collect(key, new Text(sum.toString()));
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

		sLogger.info("Tool: AuthSorter");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(SortByAuthRank.class);
		conf.setJobName("AuthSort");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		// FileInputFormat.addInputPath(conf, new
		// Path("/tmp/ccc_mmcgrath/tweets2"));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(ASortMapper.class);
		;
		conf.setReducerClass(ASortReducer.class);

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

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SortByAuthRank(),
				args);
		System.exit(res);
	}

}
