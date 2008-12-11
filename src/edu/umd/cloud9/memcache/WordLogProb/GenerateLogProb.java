package edu.umd.cloud9.memcache.WordLogProb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class GenerateLogProb {
	private static long totalWords=0;
	public static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, LongWritable> {

//		reuse objects to save overhead of object creation
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();


		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class MyReducer extends MapReduceBase implements
	Reducer<Text, LongWritable, Text, FloatWritable> {

//		reuse objects
		private final static FloatWritable SumValue = new FloatWritable();

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
			// sum up values
			long sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			double d= (double)sum/(double)totalWords;
			double tempD = Math.log(d);
			SumValue.set((float)tempD);
			output.collect(key, SumValue);
		}
	}


	public static void main(String[] args)throws IOException {
		String inputPath = "C:/users/Anand/workspace/umd-hadoop-dist/sample-input/bible+shakes.nopunc";//"/shared/sample-input/bible+shakes.nopunc";//"C:/INFM/CC/shared/bible+shakes.nopunc";
		String outputPath = "ProbSeqFile";//"/shared/sample-input/logprobs";//"D:/sample-counts/logprobs";
		
		int mapTasks = 1	;
		int reduceTasks = 1;
		
		Configuration conf1 = new Configuration();
		FileSystem fs = FileSystem.get(conf1);
		FSDataInputStream in = fs.open(new Path(inputPath));
		totalWords=countWords(inputPath);
		System.out.println("COUNT WORDS"+totalWords);
		//
		JobConf conf = new JobConf(GenerateLogProb.class);
		conf.setJobName("GenerateLogProb");
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);

	}

	public static long countWords(String inputPath) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));
		String line;
		long count=0;
		while((line=reader.readLine())!=null){
			StringTokenizer token = new StringTokenizer(line);
			while(token.hasMoreTokens()){
				token.nextToken();
				count++;
			}
		}		
		return count;
	}


}
