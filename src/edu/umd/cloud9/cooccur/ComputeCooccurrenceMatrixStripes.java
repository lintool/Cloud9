package edu.umd.cloud9.cooccur;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import edu.umd.cloud9.io.VectorInt;
import edu.umd.cloud9.util.HadoopTask;

public class ComputeCooccurrenceMatrixStripes extends HadoopTask {

	private static int mWindow = 2;

	public static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, VectorInt<Text>> {

		public void map(LongWritable key, Text line, OutputCollector<Text, VectorInt<Text>> output,
				Reporter reporter) throws IOException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				VectorInt<Text> map = new VectorInt<Text>();

				for (int j = i - mWindow; j < i + mWindow + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					Text t = new Text(terms[j]);
					if (map.containsKey(t)) {
						map.put(t, map.get(t) + 1);
					} else {
						map.put(t, 1);
					}
				}

				output.collect(new Text(term), map);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, VectorInt<Text>, Text, VectorInt<Text>> {

		public void reduce(Text key, Iterator<VectorInt<Text>> values,
				OutputCollector<Text, VectorInt<Text>> output, Reporter reporter)
				throws IOException {

			VectorInt<Text> map = null;

			while (values.hasNext()) {
				if (map == null) {
					map = values.next();
				} else {
					map.plus(values.next());
				}
			}

			output.collect(key, map);
		}
	}

	public ComputeCooccurrenceMatrixStripes(Configuration conf) {
		super(conf);
	}

	public static final String[] RequiredParameters = { "CollectionName", "InputPath",
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

		JobConf conf = new JobConf(ComputeCooccurrenceMatrixStripes.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		conf.setJobName("CooccurrenceMatrixStripes-" + collection);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		FileOutputFormat.setCompressOutput(conf, false);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VectorInt.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

	}

}
