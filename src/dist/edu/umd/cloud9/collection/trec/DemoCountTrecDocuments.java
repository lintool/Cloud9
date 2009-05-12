package edu.umd.cloud9.collection.trec;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * Simple demo program that counts all the documents in the TREC collection.
 * This provides a skeleton for MapReduce programs that process the collection.
 */
public class DemoCountTrecDocuments {

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, Text, IntWritable> {

		private final static Text sText = new Text();
		private final static IntWritable sInt = new IntWritable(1);
		private DocnoMapping mDocMapping;

		public void configure(JobConf job) {
			try {
				Path[] localFiles = DistributedCache.getLocalCacheFiles(job);

				mDocMapping = (DocnoMapping) Class.forName(job.get("DocnoMappingClass"))
						.newInstance();
				mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);

			sText.set(doc.getDocid());
			sInt.set(mDocMapping.getDocno(doc.getDocid()));
			output.collect(sText, sInt);
		}
	}

	private DemoCountTrecDocuments() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		if (args.length != 4) {
			System.out.println("usage: [input-dir] [output-dir] [mapping-file] [num-mappers]");
			System.exit(-1);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String mappingFile = args[2];
		int mapTasks = Integer.parseInt(args[3]);

		System.out.println("input dir: " + inputPath);
		System.out.println("output dir: " + outputPath);
		System.out.println("mapping file: " + mappingFile);
		System.out.println("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(DemoCountTrecDocuments.class);
		conf.setJobName("DemoCountTrecDocuments");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		conf.set("DocnoMappingClass", "edu.umd.cloud9.collection.trec.TrecDocnoMapping");

		DistributedCache.addCacheFile(new URI(mappingFile), conf);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(TrecDocumentInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

	}
}
