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

package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;
import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;

/**
 * Tool for building a document forward index for Wikipedia.
 * 
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class BuildWikipediaForwardIndex extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildWikipediaForwardIndex.class);

	private static enum Blocks { Total };

	private static class MyMapRunner implements MapRunnable<IntWritable, WikipediaPage, IntWritable, Text> {
		private static final IntWritable keyOut = new IntWritable();
		private static final Text valOut = new Text();

		private int fileno;
		private String language;
		
		public void configure(JobConf job) {
			String file = job.get("map.input.file");
			fileno = Integer.parseInt(file.substring(file.indexOf("part-") + 5));
			language = job.get("wiki.language");
		}

		public void run(RecordReader<IntWritable, WikipediaPage> input,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			IntWritable key = new IntWritable();
			WikipediaPage value = WikipediaPageFactory.createWikipediaPage(language);

			long pos = -1;
			long prevPos = -1;

			int prevDocno = 0;

			pos = input.getPos();
			while (input.next(key, value)) {
				if (prevPos != -1 && prevPos != pos) {
					LOG.info("- beginning of block at " + prevPos + ", docno:" + prevDocno + ", file:" + fileno);
					keyOut.set(prevDocno);
					valOut.set(prevPos + "\t" + fileno);
					output.collect(keyOut, valOut);
					reporter.incrCounter(Blocks.Total, 1);
				}

				prevPos = pos;
				pos = input.getPos();
				prevDocno = key.get();
			}
		}
	}

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String INDEX_FILE_OPTION = "index_file";
  private static final String LANGUAGE_OPTION = "wiki_language";
  
  @SuppressWarnings("static-access") @Override
	public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("tmp output directory").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index file").create(INDEX_FILE_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));
    
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(INDEX_FILE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Path inputPath = new Path(cmdline.getOptionValue(INPUT_OPTION));
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    String indexFile = cmdline.getOptionValue(INDEX_FILE_OPTION);

    if ( !inputPath.isAbsolute()) {
      System.err.println("Error: " + INPUT_OPTION + " must be an absolute path!");
      return -1;
    }

    String language = null;
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if(language.length()!=2){
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }
    
		JobConf conf = new JobConf(getConf(), BuildWikipediaForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - index file: " + indexFile);
		LOG.info("Note: This tool only works on block-compressed SequenceFiles!");
		LOG.info(" - language: " + language);
		 
    conf.setJobName(String.format("BuildWikipediaForwardIndex[%s: %s, %s: %s, %s: %s]",
        INPUT_OPTION, inputPath, INDEX_FILE_OPTION, indexFile, LANGUAGE_OPTION, language));

		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		if(language != null){
      conf.set("wiki.language", language);
    }
		
		conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(IdentityReducer.class);

		// delete the output directory if it exists already
		fs.delete(new Path(outputPath), true);

		RunningJob job = JobClient.runJob(conf);

		Counters counters = job.getCounters();
		int blocks = (int) counters.getCounter(Blocks.Total);

		LOG.info("number of blocks: " + blocks);

		LOG.info("Writing index file...");
		LineReader reader = new LineReader(fs.open(new Path(outputPath + "/part-00000")));
		FSDataOutputStream out = fs.create(new Path(indexFile), true);

		out.writeUTF("edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex");
		out.writeUTF(inputPath.toString());
		out.writeInt(blocks);

		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\s+");

			int docno = Integer.parseInt(arr[0]);
			int offset = Integer.parseInt(arr[1]);
			short fileno = Short.parseShort(arr[2]);

			out.writeInt(docno);
			out.writeInt(offset);
			out.writeShort(fileno);

			cnt++;

			if (cnt % 100000 == 0) {
				LOG.info(cnt + " blocks written");
			}
		}

		reader.close();
		out.close();

		if (cnt != blocks) {
			throw new RuntimeException("Error: mismatch in block count!");
		}

		return 0;
	}

	public BuildWikipediaForwardIndex() {}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildWikipediaForwardIndex(), args);
	}
}