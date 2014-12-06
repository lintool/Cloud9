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

package edu.umd.cloud9.collection;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import edu.umd.cloud9.collection.line.TextDocument;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.TrecExtractLinks.Map.LinkCounter;

/**
 * <p>
 * Tool for generating 'per-field' collections from HTML documents. The output of this
 * tool is a new collection, in TREC format (in the form of a SequenceFile<LongWritable, TextDocument>)
 * that only consists of the text contained within the target tag. This is useful for various
 * document structure and/or field-based retrieval tasks.
 * </p>
 *
 * @author fangyue
 * @author metzler
 */
public class ExtractHTMLFieldCollection extends PowerTool {
  private static final Logger LOG = Logger.getLogger(ExtractHTMLFieldCollection.class);

  public static class MyMapper extends Mapper<LongWritable, Indexable, LongWritable, TextDocument> {
    // TODO: allow this to support user-defined regular expressions, not just the "heading" one pre-defined here
    public static class HeadingTagFilter implements NodeFilter {
      private static final long serialVersionUID = 3848416345122090905L;
      private final Pattern pattern = Pattern.compile("h[123456]", Pattern.CASE_INSENSITIVE);

      public boolean accept(Node node) {
        return (pattern.matcher(node.getText()).matches());
      }
    }

    private static String tag;

    private static final Parser parser = new Parser();
    private static NodeFilter filter;

    private static final LongWritable myKey = new LongWritable();
    private static final TextDocument myValue = new TextDocument();

    private static final StringBuffer strBuf = new StringBuffer();

    @Override
    public void setup(Mapper<LongWritable, Indexable, LongWritable, TextDocument>.Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      tag = conf.get("Cloud9.TargetTag");

      if (tag.equalsIgnoreCase("heading")) {
        filter = new HeadingTagFilter();
      } else {
        filter = new TagNameFilter(tag);
      }
    }

    @Override
    public void map(LongWritable key, Indexable doc, Mapper<LongWritable, Indexable, LongWritable, TextDocument>.Context context) throws IOException, InterruptedException {
      context.getCounter(LinkCounter.INPUT_DOCS).increment(1);

      if(doc.getDocid() == null || doc.getContent() == null) {
        return;
      }

      myKey.set(key.get());

      NodeList nl;
      try {
        // initialize HTML parser
        parser.setInputHTML(doc.getContent());

        // parse the document
        nl = parser.parse(filter);
      } catch (ParserException e) {
        context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
        myValue.setDocid(doc.getDocid());
        myValue.setContent("<DOC>\n<DOCNO>" + doc.getDocid() + "</DOCNO>\n<DOC>");
        context.write(myKey, myValue);
        return;
      } catch (StackOverflowError e) {
        context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
        myValue.setDocid(doc.getDocid());
        myValue.setContent("<DOC>\n<DOCNO>" + doc.getDocid() + "</DOCNO>\n<DOC>");
        context.write(myKey, myValue);
        return;
      }

      strBuf.setLength(0);
      strBuf.append("<DOC>\n<DOCNO>");
      strBuf.append(doc.getDocid());
      strBuf.append("</DOCNO>\n");

      for (int i = 0; i < nl.size(); i++) {
        strBuf.append(nl.elementAt(i).toHtml()).append("\n");
      }
      strBuf.append("</DOC>\n");

      // create output document
      myValue.setDocid(doc.getDocid());
      myValue.setContent(strBuf.toString());

      // emit
      context.write(myKey, myValue);

      // bookkeeping
      context.getCounter(LinkCounter.OUTPUT_DOCS).increment(1);
    }
  }

  public static final String[] RequiredParameters = { "Cloud9.InputPath", "Cloud9.InputFormat", "Cloud9.OutputPath", "Cloud9.TargetTag" };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public ExtractHTMLFieldCollection(Configuration conf) {
    super(conf);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public int runTool() throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf);

    String inputPath = conf.get("Cloud9.InputPath");
    String inputFormat = conf.get("Cloud9.InputFormat");
    String outputPath = conf.get("Cloud9.OutputPath");
    String tag = conf.get("Cloud9.TargetTag");

    job.setJobName("ExtractFieldCollection");

    job.setJarByClass(ExtractHTMLFieldCollection.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(200);

    job.setInputFormatClass((Class<? extends InputFormat>) Class.forName(inputFormat));
    recursivelyAddInputPaths(job, inputPath);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TextDocument.class);

    LOG.info("ExtractFieldCollection - " + tag);
    LOG.info(" - Input path: " + inputPath);
    LOG.info(" - Input format: " + inputFormat);
    LOG.info(" - Output path: " + outputPath);
    LOG.info(" - Target tag: " + tag);

    job.waitForCompletion(true);
    return 0;
  }

  public static void recursivelyAddInputPaths(Job job, String path) throws IOException {
    FileSystem fs;
    try {
      fs = FileSystem.get(new URI(path), job.getConfiguration());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Error recursively adding path -- " + path);
    }

    FileStatus[] ls = fs.listStatus(new Path(path));
    for (FileStatus status : ls) {
      // skip anything that starts with an underscore, as it often indicates
      // a log directory or another special type of Hadoop file
      if (status.getPath().getName().startsWith("_")) {
        continue;
      }

      if (status.isDir()) {
        recursivelyAddInputPaths(job, status.getPath().toString());
      } else {
        FileInputFormat.addInputPath(job, status.getPath());
      }
    }
  }

  public static void main(String [] args) throws Exception {
    Configuration conf = new Configuration();

    if(args.length != 4) {
      System.err.println("Usage: ExtractFieldCollection [input-path] [input-format] [output-path] [target-tag]");
      System.exit(-1);
    }

    conf.set("Cloud9.InputPath", args[0]);
    conf.set("Cloud9.InputFormat", args[1]);
    conf.set("Cloud9.OutputPath", args[2]);
    conf.set("Cloud9.TargetTag", args[3]);

    int res = ToolRunner.run(conf, new ExtractHTMLFieldCollection(conf), args);
    System.exit(res);
  }
}
