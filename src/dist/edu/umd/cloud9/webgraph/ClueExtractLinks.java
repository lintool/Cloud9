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

package edu.umd.cloud9.webgraph;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;
import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.tags.BaseHrefTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

/**
 * 
 * @author Nima Asadi
 * 
 */
public class ClueExtractLinks extends PowerTool {
  private static final Logger LOG = Logger.getLogger(ClueExtractLinks.class);

  public static class Map extends MapReduceBase implements
      Mapper<IntWritable, ClueWarcRecord, Text, ArrayListWritable<AnchorText>> {

    public static enum LinkCounter {
      INPUT_DOCS, // number of input documents
      OUTPUT_DOCS, // number of output documents
      INVALID_DOCNO, // number of malformed documents
      INVALID_URL, // number of malformed URLs
      TEXT_TOO_LONG, // number of lines of anchor text that are abnormally
      // long
      PARSER_FAILED
      // number of times the HTML parser fails
    };

    private static String base; // base URL for current document
    private static String baseHost;
    private static int docno; // docno of current document

    private static final Text keyWord = new Text(); // output key for the
    // mappers
    private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
    // output value for the mappers

    private static final ClueWarcDocnoMapping docnoMapping = new ClueWarcDocnoMapping();
    // docno mapping file

    private static final Parser parser = new Parser();
    private static final NodeFilter filter = new NodeClassFilter(LinkTag.class);;
    private static NodeList list;

    private static boolean includeInternalLinks;

    private static AnchorTextNormalizer normalizer;

    public void configure(JobConf job) {
      Path[] localFiles;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
      } catch (IOException e) {
        throw new RuntimeException("Local cache files not read properly.");
      }

      try {
        docnoMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }

      includeInternalLinks = job.getBoolean("Cloud9.IncludeInternalLinks", false);

      try {
        normalizer = (AnchorTextNormalizer) Class.forName(job.get("Cloud9.AnchorTextNormalizer"))
            .newInstance();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing AnchorTextNormalizer");
      }
    }

    public void map(IntWritable key, ClueWarcRecord doc,
        OutputCollector<Text, ArrayListWritable<AnchorText>> output, Reporter reporter)
        throws IOException {

      reporter.incrCounter(LinkCounter.INPUT_DOCS, 1);

      try {
        docno = docnoMapping.getDocno(doc.getHeaderMetadataItem("WARC-TREC-ID"));
      } catch (NullPointerException e) {
        // Discard documents with an invalid document number
        reporter.incrCounter(LinkCounter.INVALID_DOCNO, 1);
        return;
      }

      try {
        base = doc.getHeaderMetadataItem("WARC-Target-URI");
      } catch (NullPointerException e) {
        // Discard documents with which there is no URL associated
        reporter.incrCounter(LinkCounter.INVALID_URL, 1);
        return;
      }

      if (base == null) {
        reporter.incrCounter(LinkCounter.INVALID_URL, 1);
        return;
      }

      arrayList.clear();
      arrayList.add(new AnchorText(AnchorTextConstants.Type.DOCNO_FIELD.val,
          AnchorTextConstants.EMPTY_STRING, docno));
      keyWord.set(base);
      output.collect(keyWord, arrayList);
      arrayList.clear();

      // keeping track of the number of documents that have actually been
      // processed
      reporter.incrCounter(LinkCounter.OUTPUT_DOCS, 1);

      try {
        baseHost = new URI(base).getHost();
      } catch (Exception e) {
        reporter.incrCounter(LinkCounter.INVALID_URL, 1);
        return;
      }

      if (baseHost == null) {
        reporter.incrCounter(LinkCounter.INVALID_URL, 1);
        return;
      }

      try {
        parser.setInputHTML(doc.getContent()); // initializing the
        // parser with new HTML
        // content

        // Setting base URL for the current document
        NodeList nl = parser.parse(null);
        BaseHrefTag baseTag = new BaseHrefTag();
        baseTag.setBaseUrl(base);
        nl.add(baseTag);

        // re-initializing the parser with the fixed content
        parser.setInputHTML(nl.toHtml());

        // listing all LinkTag nodes
        list = parser.extractAllNodesThatMatch(filter);
      } catch (ParserException e) {
        reporter.incrCounter(LinkCounter.PARSER_FAILED, 1);
        return;
      } catch (StackOverflowError e) {
        reporter.incrCounter(LinkCounter.PARSER_FAILED, 1);
        return;
      }

      for (int i = 0; i < list.size(); i++) {
        LinkTag link = (LinkTag) list.elementAt(i);
        String anchor = link.getLinkText();
        String url = link.extractLink();

        if (url == null)
          continue;

        if (url.equals(base)) // discard self links
          continue;

        String host = null;
        try {
          host = new URI(url).getHost();
        } catch (Exception e) {
          continue;
        }

        if (host == null)
          continue;

        if (anchor == null)
          anchor = "";

        // normalizing the anchor text
        anchor = normalizer.process(anchor);

        arrayList.clear();
        if (baseHost.equals(host)) {

          if (!includeInternalLinks)
            continue;

          arrayList
              .add(new AnchorText(AnchorTextConstants.Type.INTERNAL_IN_LINK.val, anchor, docno));

        } else {
          arrayList
              .add(new AnchorText(AnchorTextConstants.Type.EXTERNAL_IN_LINK.val, anchor, docno));
        }

        try {
          keyWord.set(url);
          output.collect(keyWord, arrayList);
        } catch (UTFDataFormatException e) {
          reporter.incrCounter(LinkCounter.TEXT_TOO_LONG, 1);

          keyWord.set(url);
          byte flag = arrayList.get(0).getType();
          arrayList.clear();
          arrayList.add(new AnchorText(flag, AnchorTextConstants.EMPTY_STRING, docno));
          output.collect(keyWord, arrayList);
        }

      }
    }
  }

  public static class Reduce extends MapReduceBase implements
      Reducer<Text, ArrayListWritable<AnchorText>, Text, ArrayListWritable<AnchorText>> {

    private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
    private static ArrayListWritable<AnchorText> packet;
    private static boolean pushed;

    public void reduce(Text key, Iterator<ArrayListWritable<AnchorText>> values,
        OutputCollector<Text, ArrayListWritable<AnchorText>> output, Reporter reporter)
        throws IOException {

      arrayList.clear();

      while (values.hasNext()) {
        packet = values.next();

        for (AnchorText data : packet) {

          pushed = false;

          for (int i = 0; i < arrayList.size(); i++) {
            if (arrayList.get(i).equalsIgnoreSources(data)) {
              arrayList.get(i).addDocumentsFrom(data);
              pushed = true;
              break;
            }
          }

          if (!pushed)
            arrayList.add(data.clone());
        }
      }

      output.collect(key, arrayList);
    }
  }

  public static final String[] RequiredParameters = { "Cloud9.InputPath", "Cloud9.OutputPath",
      "Cloud9.Mappers", "Cloud9.Reducers", "Cloud9.DocnoMappingFile",
      "Cloud9.IncludeInternalLinks", "Cloud9.AnchorTextNormalizer", };

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public ClueExtractLinks(Configuration conf) {
    super(conf);
  }

  public int runTool() throws Exception {

    JobConf conf = new JobConf(getConf(), ClueExtractLinks.class);
    FileSystem fs = FileSystem.get(conf);

    int numMappers = conf.getInt("Cloud9.Mappers", 1);
    int numReducers = conf.getInt("Cloud9.Reducers", 200);

    String inputPath = conf.get("Cloud9.InputPath");
    String outputPath = conf.get("Cloud9.OutputPath");
    String mappingFile = conf.get("Cloud9.DocnoMappingFile");

    if (!fs.exists(new Path(mappingFile)))
      throw new RuntimeException("Error: Docno mapping data file " + mappingFile
          + " doesn't exist!");

    DistributedCache.addCacheFile(new URI(mappingFile), conf);

    conf.setJobName("ClueExtractLinks");
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setInt("mapred.task.timeout", 60000000);

    conf.setNumMapTasks(numMappers);
    conf.setNumReduceTasks(numReducers);
    // TODO: to read!!
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(ArrayListWritable.class);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

    SequenceFileInputFormat.setInputPaths(conf, inputPath);

    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    LOG.info("ClueExtractLinks");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - mapping file: " + mappingFile);
    LOG.info(" - include internal links? " + conf.getBoolean("Cloud9.IncludeInternalLinks", false));

    if (!fs.exists(new Path(outputPath))) {
      JobClient.runJob(conf);
    } else {
      LOG.info(outputPath + " already exists! Skipping this step...");
    }

    return 0;
  }
}
