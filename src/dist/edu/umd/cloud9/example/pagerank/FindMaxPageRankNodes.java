/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.example.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class FindMaxPageRankNodes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(FindMaxPageRankNodes.class);

	private static class MyMapper extends Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {
    private TopNScoredObjects<Integer> queue;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable>.Context context)
        throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queue = new TopNScoredObjects<Integer>(k);
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      queue.add(node.getNodeId(), node.getPageRank());
    }

    @Override
    public void cleanup(Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable>.Context context)
        throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (PairOfObjectFloat<Integer> pair : queue.extractAll()) {
        key.set(pair.getLeftElement());
        value.set(pair.getRightElement());
        context.write(key, value);
      }
    }
	}

	private static class MyReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    private TopNScoredObjects<Integer> queue;

    @Override
    public void setup(Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>.Context context)
        throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queue = new TopNScoredObjects<Integer>(k);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queue.add(nid.get(), iter.next().get());

      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>.Context context)
        throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (PairOfObjectFloat<Integer> pair : queue.extractAll()) {
        key.set(pair.getLeftElement());
        value.set(pair.getRightElement());
        context.write(key, value);
      }
    }
	}

	public FindMaxPageRankNodes() {}

	private static int printUsage() {
		System.out.println("usage: [input] [output] [n]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int n = Integer.parseInt(args[2]);
		
		LOG.info("Tool name: FindMaxPageRankNodes");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		LOG.info(" - n: " + n);

		Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);

		Job job = new Job(conf, "FindMaxPageRankNodes");
		job.setJarByClass(FindMaxPageRankNodes.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FindMaxPageRankNodes(), args);
		System.exit(res);
	}
}
