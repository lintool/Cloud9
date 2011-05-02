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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;
import edu.umd.cloud9.util.map.HMapIF;
import edu.umd.cloud9.util.map.MapIF;

/**
 * <p>
 * Main driver program for running the Schimmy implementation of PageRank.
 * Command-line arguments are as follows:
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
 * @see RunPageRankBasic
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPageRankSchimmy extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(RunPageRankSchimmy.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final FloatWritable intermediateMass = new FloatWritable();

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        float mass = node.getPageRank() - (float) StrictMath.log(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.set(mass);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Mapper with in-mapper combiner optimization.
  private static class MapWithInMapperCombiningClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {

    // For buffering PageRank mass contributes keyed by destination node.
    private static HMapIF map = new HMapIF();

    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {

      int massMessages = 0;
      int massMessagesSaved = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        float mass = node.getPageRank() - (float) StrictMath.log(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          int neighbor = list.get(i);

          if (map.containsKey(neighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            massMessagesSaved++;
            map.put(neighbor, sumLogProbs(map.get(neighbor), mass));
          } else {
            // New destination node; add new entry in map.
            massMessages++;
            map.put(neighbor, mass);
          }
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
      context.getCounter(PageRank.massMessagesSaved).increment(massMessagesSaved);
    }

    @Override
    public void cleanup(
        Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable>.Context context)
        throws IOException, InterruptedException {
      // Now emit the messages all at once.
      IntWritable k = new IntWritable();
      FloatWritable v = new FloatWritable();

      for (MapIF.Entry e : map.entrySet()) {
        k.set(e.getKey());
        v.set(e.getValue());

        context.write(k, v);
      }
    }
  }

  // Combiner: sums partial PageRank contributions.
  private static class CombineClass extends
      Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    private static final FloatWritable intermediateMass = new FloatWritable();

    @Override
    public void reduce(IntWritable nid, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      float mass = Float.NEGATIVE_INFINITY;
      for (FloatWritable n : values) {
        // Accumulate PageRank mass contributions
        mass = sumLogProbs(mass, n.get());

        massMessages++;
      }

      // emit aggregated results
      if (massMessages > 0) {
        intermediateMass.set(mass);
        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, FloatWritable, IntWritable, PageRankNode> {

    private float totalMass = Float.NEGATIVE_INFINITY;

    private SequenceFile.Reader reader;

    private IntWritable hdfsNid = new IntWritable();
    private PageRankNode hdfsNode = new PageRankNode();

    private boolean hdfsAhead = false;

    @Override
    public void setup(Reducer<IntWritable, FloatWritable, IntWritable, PageRankNode>.Context context)
        throws IOException {
      // We're going to open up the file on HDFS that has corresponding node structures. To do this,
      // we get the task id and map it to the corresponding part.
      Configuration conf = context.getConfiguration();

      String taskId = conf.get("mapred.task.id");
      Preconditions.checkNotNull(taskId);

      // The partition mapping is passed in from the driver.
      String mapping = conf.get("PartitionMapping");
      Preconditions.checkNotNull(mapping);

      Map<Integer, String> map = new HashMap<Integer, String>();
      for (String s : mapping.split(";")) {
        String[] arr = s.split("=");

        LOG.info(arr[0] + "\t" + arr[1]);

        map.put(Integer.parseInt(arr[0]), arr[1]);
      }

      // Get the part number.
      int partno = Integer.parseInt(taskId.substring(taskId.length() - 7, taskId.length() - 2));
      String f = map.get(partno);

      LOG.info("task id: " + taskId);
      LOG.info("partno: " + partno);
      LOG.info("file: " + f);

      // Try and open the node structures...
      try {
        FileSystem fs = FileSystem.get(conf);
        reader = new SequenceFile.Reader(fs, new Path(f), conf);
      } catch (IOException e) {
        throw new RuntimeException("Couldn't open " + f + " for partno: " + partno + " within: "
            + taskId);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      // The basic algorithm is a merge sort between node structures on HDFS and intermediate
      // key-value pairs coming into this reducer (where the keys are the node ids). Both are
      // sorted, and the reducer is "pushed" intermediate key-value pairs, so the algorithm boils
      // down to properly advancing the node structures file on HDFS.

      // The HDFS node structure file is ahead. This means the incoming node ids don't have
      // corresponding node structure (i.e., messages addressed to non-existent nodes). This may
      // happen if the adjacency lists point to nodes that don't exist. Do nothing.
      if (hdfsNid.get() > nid.get()) {
        return;
      }

      // We need to advance the HDFS node structure file.
      if (hdfsNid.get() < nid.get()) {
        if (hdfsAhead) {
          // If we get here, it means that no messages were sent to a particular node in the HDFS
          // node structure file. So we want to emit this node structure.
          hdfsNode.setPageRank(Float.NEGATIVE_INFINITY);
          context.write(hdfsNid, hdfsNode);
          hdfsAhead = false;
        }

        // We're now going to advance the HDFS node structure until we get to the node id of the
        // current message we're processing...
        while (reader.next(hdfsNid, hdfsNode)) {
          if (hdfsNid.get() == nid.get()) {
            // Found it!
            break;
          }

          // If we go past the incoming node id in the HDFS node structure file, then it means that
          // no corresponding no structure exist. That is, a message was sent to a non-existent
          // node: this may happen if adjacency lists point to nodes that don't exist.
          if (hdfsNid.get() > nid.get()) {
            // We want to note that we've gotten ahead in the HDFS node structure file, and need to
            // wait for the incoming key-value pairs to "catch up".
            hdfsAhead = true;

            return;
          }

          // This is a node that has not messages sent to it... we don't want to node the node
          // structure, so just emit.
          hdfsNode.setPageRank(Float.NEGATIVE_INFINITY);
          context.write(hdfsNid, hdfsNode);
        }

        // If we get here, it means that the reader ran out of nodes, i.e., next method returned
        // false. This means that the messages were addressed to non-existent nodes.
        if (hdfsNid.get() != nid.get()) {
          return;
        }
      }

      int massMessagesReceived = 0;
      float mass = Float.NEGATIVE_INFINITY;

      // Now we process the messages: sum up PageRank mass contributions.
      for (FloatWritable f : values) {
        float n = f.get();
        massMessagesReceived++;

        mass = sumLogProbs(mass, n);
      }

      totalMass = sumLogProbs(totalMass, mass);

      // Populate the node structure with the updated PageRank value.
      hdfsNode.setPageRank(mass);

      // Emit!
      context.write(nid, hdfsNode);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      hdfsAhead = false;
    }

    @Override
    public void cleanup(
        Reducer<IntWritable, FloatWritable, IntWritable, PageRankNode>.Context context)
        throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      out.writeFloat(totalMass);
      out.close();

      // If the HDFS node structure file is ahead, we want to emit the current node structure.
      if (hdfsAhead) {
        hdfsNode.setPageRank(Float.NEGATIVE_INFINITY);
        context.write(hdfsNid, hdfsNode);
        hdfsAhead = false;
      }

      // We have to write out the rest of the nodes we haven't finished reading yet (i.e., these are
      // the ones who don't have any messages sent to them)
      while (reader.next(hdfsNid, hdfsNode)) {
        hdfsNode.setPageRank(Float.NEGATIVE_INFINITY);
        context.write(hdfsNid, hdfsNode);
      }

      reader.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    private float missingMass = 0.0f;
    private int nodeCnt = 0;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context)
        throws IOException {
      Configuration conf = context.getConfiguration();

      missingMass = conf.getFloat("MissingMass", 0.0f);
      nodeCnt = conf.getInt("NodeCount", 0);
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {

      float p = node.getPageRank();

      float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
      float link = (float) Math.log(1.0f - ALPHA)
          + sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));

      p = sumLogProbs(jump, link);
      node.setPageRank(p);

      context.write(nid, node);
    }
  }

	private static float ALPHA = 0.15f;    // Random jump factor.
	private static final NumberFormat FORMAT = new DecimalFormat("0000");

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new RunPageRankSchimmy(), args);
		System.exit(res);
	}

	public RunPageRankSchimmy() {
	}

	private static int printUsage() {
		System.out.println("usage: [basePath] [numNodes] [start] [end] [useCombiner?] [useInMapCombiner?] [useRange?]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 7) {
			System.err.println("Invalid number of args: " + args.length);
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

		LOG.info("Tool name: RunPageRank");
		LOG.info(" - basePath: " + basePath);
		LOG.info(" - numNodes: " + n);
		LOG.info(" - start iteration: " + s);
		LOG.info(" - end iteration: " + e);
		LOG.info(" - useCombiner?: " + useCombiner);
		LOG.info(" - useInMapCombiner?: " + useInmapCombiner);
		LOG.info(" - useRange?: " + useRange);

		// iterate PageRank
		for (int i = s; i < e; i++) {
			iteratePageRank(basePath, i, i + 1, n, useCombiner, useInmapCombiner, useRange);
		}

		return 0;
	}

	// Run each iteration.
	private void iteratePageRank(String path, int i, int j, int n, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange) throws Exception {
		// Each iteration consists of two phases (two MapReduce jobs).

		// Job1: distribute PageRank mass along outgoing edges.
		float mass = phase1(path, i, j, n, useCombiner, useInmapCombiner, useRange);

		// Find out how much PageRank mass got lost at the dangling nodes.
		float missing = 1.0f - (float) StrictMath.exp(mass);
		if ( missing < 0.0f ) {
			missing = 0.0f;
		}

		// Job2: distribute missing mass, take care of random jump factor.
		phase2(path, i, j, n, missing);
	}

	private float phase1(String path, int i, int j, int n, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange) throws Exception {
		Configuration conf = getConf();

		String in = path + "/iter" + FORMAT.format(i);
		String out = path + "/iter" + FORMAT.format(j) + "t";
		String outm = out + "-mass";

		FileSystem fs = FileSystem.get(conf);
		
		// We need to actually count the number of part files to get the number
		// of partitions (because the directory might contain _log).
		int numPartitions = 0;
		for (FileStatus s : FileSystem.get(conf).listStatus(new Path(in))) {
			if (s.getPath().getName().contains("part-"))
				numPartitions++;
		}
		
		conf.setInt("NodeCount", n);

		Partitioner<IntWritable, Writable> p = null;

		if (useRange) {
			p = new RangePartitioner();
			((Configurable) p).setConf(conf);
		} else {
			p = new HashPartitioner<IntWritable, Writable>();
		}

		// This is really annoying: the mapping between the partition numbers on
		// disk (i.e., part-XXXX) and what partition the file contains (i.e.,
		// key.hash % #reducer) is arbitrary... so this means that we need to
		// open up each partition, peek inside to find out.
		IntWritable key = new IntWritable();
		PageRankNode value = new PageRankNode();
		FileStatus[] status = fs.listStatus(new Path(in));

		StringBuilder sb = new StringBuilder();

		for (FileStatus f : status) {
			if (f.getPath().getName().contains("_logs"))
				continue;

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), conf);

			reader.next(key, value);
			int np = p.getPartition(key, value, numPartitions);
			reader.close();

			LOG.info(f.getPath() + "\t" + np);
			sb.append(np + "=" + f.getPath() + ";");
		}

		LOG.info(sb.toString().trim());

		LOG.info("PageRankSchimmy: iteration " + j + ": Phase1");
		LOG.info(" - input: " + in);
		LOG.info(" - output: " + out);
		LOG.info(" - nodeCnt: " + n);
		LOG.info(" - useCombiner: " + useCombiner);
		LOG.info(" - useInmapCombiner: " + useInmapCombiner);
		LOG.info(" - numPartitions: " + numPartitions);
		LOG.info(" - useRange: " + useRange);
		LOG.info("computed number of partitions: " + numPartitions);

		int numReduceTasks = numPartitions;

		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.set("PageRankMassPath", outm);
		conf.set("BasePath", in);
		conf.set("PartitionMapping", sb.toString().trim());

    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

		Job job = new Job(conf, "PageRankSchimmy:iteration" + j + ":Phase1");
		job.setJarByClass(RunPageRankSchimmy.class);

    job.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNode.class);

		if (useInmapCombiner) {
			job.setMapperClass(MapWithInMapperCombiningClass.class);
		} else {
			job.setMapperClass(MapClass.class);
		}

		if (useCombiner) {
			job.setCombinerClass(CombineClass.class);
		}

		if (useRange) {
			job.setPartitionerClass(RangePartitioner.class);
		}

		job.setReducerClass(ReduceClass.class);

		FileSystem.get(conf).delete(new Path(out), true);
		FileSystem.get(conf).delete(new Path(outm), true);

		job.waitForCompletion(true);

		float mass = Float.NEGATIVE_INFINITY;
		for (FileStatus f : fs.listStatus(new Path(outm))) {
			FSDataInputStream fin = fs.open(f.getPath());
			mass = sumLogProbs(mass, fin.readFloat());
			fin.close();
		}

		return mass;
	}

	private void phase2(String path, int i, int j, int n, float missing) throws Exception {
	  Configuration conf = getConf();

		LOG.info("missing PageRank mass: " + missing);
		LOG.info("number of nodes: " + n);

		String in = path + "/iter" + FORMAT.format(j) + "t";
		String out = path + "/iter" + FORMAT.format(j);

		LOG.info("PageRankSchimmy: iteration " + j + ": Phase2");
		LOG.info(" - input: " + in);
		LOG.info(" - output: " + out);

		Job job = new Job(conf, "PageRankSchimmy:iteration" + j + ":Phase2");
		job.setJarByClass(RunPageRankSchimmy.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNode.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNode.class);

		job.setMapperClass(MapPageRankMassDistributionClass.class);

		conf.setFloat("MissingMass", (float) missing);
		conf.setInt("NodeCount", n);

		FileSystem.get(conf).delete(new Path(out), true);

		job.waitForCompletion(true);
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
}
