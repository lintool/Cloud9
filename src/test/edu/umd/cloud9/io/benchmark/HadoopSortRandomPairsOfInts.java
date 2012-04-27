package edu.umd.cloud9.io.benchmark;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * <p>
 * Benchmark for comparing Hadoop sorting with and without the
 * WritableComparator optimization. Task is sorting the one million PairOfInts
 * created by {@link GenerateRandomPairsOfInts}.
 * </p>
 * 
 * <p>
 * Comparison of sort speed with and without WritableComparator optimization, on
 * Hadoop 0.17.2 in local mode, Java 1.5, MacBookPro (2.6 GHz, 2GB RAM) running
 * Windows XP/Cygwin. Benchmark conducted 10/16/2008. Running times reported in
 * seconds.
 * </p>
 * 
 * <table border="1" cellpadding="5">
 * <tr>
 * <td><b>Trial</b></td>
 * <td><b>Without Optimization</b></td>
 * <td><b>With Optimization</b></td>
 * </tr>
 * <tr>
 * <td>1</td>
 * <td>36.406</td>
 * <td>21.344</td>
 * </tr>
 * <tr>
 * <td>2</td>
 * <td>35.562</td>
 * <td>21.407</td>
 * </tr>
 * <tr>
 * <td>3</td>
 * <td>36.532</td>
 * <td>22.453</td>
 * </tr>
 * <tr>
 * <td>4</td>
 * <td>36.39</td>
 * <td>22.484</td>
 * </tr>
 * <tr>
 * <td>5</td>
 * <td>36.453</td>
 * <td>21.375</td>
 * </tr>
 * <tr>
 * <td>6</td>
 * <td>35.5</td>
 * <td>22.484</td>
 * </tr>
 * <tr>
 * <td>7</td>
 * <td>36.391</td>
 * <td>22.562</td>
 * </tr>
 * <tr>
 * <td>8</td>
 * <td>36.323</td>
 * <td>22.484</td>
 * </tr>
 * <tr>
 * <td>9</td>
 * <td>35.906</td>
 * <td>22.422</td>
 * </tr>
 * <tr>
 * <td>10</td>
 * <td>36.453</td>
 * <td>22.344</td>
 * </tr>
 * <tr>
 * <td><b>mean</b></td>
 * <td>36.19 [35.95, 36.43]</td>
 * <td>22.14 [21.81, 22.46]</td>
 * </tr>
 * </table>
 * 
 * <p>
 * Numbers in square brackets denote 95% confidence intervals.
 * </p>
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopSortRandomPairsOfInts {

	private HadoopSortRandomPairsOfInts() {
	}

	/**
	 * Runs this benchmark.
	 */
	public static void main(String[] args) throws IOException {
		String inputPath = "random-pairs.seq";
		String outputPath = "random-pairs.sorted";
		int numMapTasks = 1;
		int numReduceTasks = 1;

		JobConf conf = new JobConf(HadoopSortRandomPairsOfInts.class);
		conf.setJobName("SortRandomPairsOfInts");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(PairOfInts.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(IdentityMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(IdentityReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		JobClient.runJob(conf);

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Job took " + duration + " seconds");

	}
}
