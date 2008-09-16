package edu.umd.cloud9.cooccur;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.util.HadoopTask;

public class ComputeCooccurrenceMatrixPairs extends HadoopTask {

	private static int mWindow = 2;

	public static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

		PairOfStrings pair = new PairOfStrings();
		IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text line,
				OutputCollector<PairOfStrings, IntWritable> output, Reporter reporter)
				throws IOException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// skip empty tokens
				if (term.length() == 0)
					continue;

				for (int j = i - mWindow; j < i + mWindow + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					// skip empty tokens
					if (terms[j].length() == 0)
						continue;

					pair.set(term, terms[j]);
					output.collect(pair, one);
				}
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {

		private final static IntWritable SumValue = new IntWritable();

		public void reduce(PairOfStrings key, Iterator<IntWritable> values,
				OutputCollector<PairOfStrings, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			SumValue.set(sum);
			output.collect(key, SumValue);
		}
	}

	public ComputeCooccurrenceMatrixPairs(Configuration conf) {
		super(conf);
	}

	private static final String[] RequiredParameters = { "CollectionName", "InputPath",
			"OutputPath", "NumMapTasks", "NumReduceTasks", "Window" };

	public String[] getRequiredParameters() {
		return RequiredParameters;
	}

	public void runTask() throws Exception {
		String collection = getConf().get("CollectionName");
		String inputPath = getConf().get("InputPath");
		String outputPath = getConf().get("OutputPath");

		int mapTasks = getConf().getInt("NumMapTasks", 0);
		int reduceTasks = getConf().getInt("NumReduceTasks", 0);
		mWindow = getConf().getInt("Window", 0);

		JobConf conf = new JobConf(ComputeCooccurrenceMatrixPairs.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		conf.setJobName("CooccurrenceMatrixPairs-" + collection);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		FileOutputFormat.setCompressOutput(conf, false);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(PairOfStrings.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

	}
}
