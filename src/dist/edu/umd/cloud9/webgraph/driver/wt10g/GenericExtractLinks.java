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

package edu.umd.cloud9.webgraph.driver.wt10g;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.BaseHrefTag;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.generic.WebDocument;
import edu.umd.cloud9.collection.generic.WebDocumentInputFormat;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;
import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

/**
 * 
 * @author Nima Asadi
 * @author Fangyue Wang
 * @author metzler
 * 
 */

public class GenericExtractLinks extends PowerTool {

  private static final Logger LOG = Logger.getLogger(GenericExtractLinks.class);

  public static class Map extends Mapper<LongWritable, WebDocument, Text, ArrayListWritable<AnchorText>> {

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

    private static DocnoMapping docnoMapping = null;

    private static final Parser parser = new Parser();
    private static final NodeFilter filter = new NodeClassFilter(LinkTag.class);
    private static NodeList list;

    private static boolean includeInternalLinks;

    private static AnchorTextNormalizer normalizer;

    @Override
    public void setup(Mapper<LongWritable, WebDocument, Text, ArrayListWritable<AnchorText>>.Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      String docnoMappingClass = conf.get("Cloud9.DocnoMappingClass");
      try {
        docnoMapping = (DocnoMapping)Class.forName(docnoMappingClass).newInstance();
      }
      catch(Exception e) {
        throw new RuntimeException("Error initializing DocnoMapping class!");
      }
      
      String docnoMappingFile = conf.get("Cloud9.DocnoMappingFile", null);
      if(docnoMappingFile != null) {
        Path docnoMappingPath = null;
        try {
          Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
          if(localFiles != null) {
            docnoMappingPath = localFiles[0];
          }
          else {
            docnoMappingPath = new Path(conf.get("Cloud9.DocnoMappingFile"));
          }
        }
        catch (IOException e) {
          throw new RuntimeException("Unable to find DocnoMappingFile!");
        }

        try {
          docnoMapping.loadMapping(docnoMappingPath, FileSystem.getLocal(conf));
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Error initializing DocnoMapping!");
        }
      }

      includeInternalLinks = conf.getBoolean("Cloud9.IncludeInternalLinks", false);

      try {
        normalizer = (AnchorTextNormalizer) Class.forName(conf.get("Cloud9.AnchorTextNormalizer")).newInstance();
      }
      catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing AnchorTextNormalizer");
      }
    }

    @Override
    public void map(LongWritable key, WebDocument doc, Mapper<LongWritable, WebDocument, Text, ArrayListWritable<AnchorText>>.Context context) throws IOException, InterruptedException {
      context.getCounter(LinkCounter.INPUT_DOCS).increment(1);

      try
      {
        docno = docnoMapping.getDocno(doc.getDocid());
      }
      catch (NullPointerException e)
      {
        // Discard documents with an invalid document number
        context.getCounter(LinkCounter.INVALID_DOCNO).increment(1);
        return;
      }

      try {
        base = normalizeURL(doc.getURL());
      }
      catch (Exception e) {
        // Discard documents with which there is no URL associated
        context.getCounter(LinkCounter.INVALID_URL).increment(1);
        return;
      }

      if (base == null) {
        context.getCounter(LinkCounter.INVALID_URL).increment(1);
        return;
      }

      try {
        baseHost = new URI(base).getHost();
      }
      catch (Exception e) {
        context.getCounter(LinkCounter.INVALID_URL).increment(1);
        return;
      }

      if (baseHost == null) {
        context.getCounter(LinkCounter.INVALID_URL).increment(1);
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
      }
      catch (ParserException e) {
        context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
        return;
      }
      catch (StackOverflowError e) {
        context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
        return;
      }

      for (int i = 0; i < list.size(); i++) {
        LinkTag link = (LinkTag) list.elementAt(i);
        String anchor = link.getLinkText();
        String url = normalizeURL(link.extractLink());

        if (url == null) {
          continue;
        }

        if (url.equals(base)) { // discard self links
          continue;
        }

        String host = null;
        try {
          host = new URI(url).getHost();
        }
        catch (Exception e) {
          continue;
        }

        if (host == null) {
          continue;
        }

        if (anchor == null) {
          anchor = "";
        }

        // normalizing the anchor text
        anchor = normalizer.process(anchor);

        arrayList.clear();
        if (baseHost.equals(host)) {

          if (!includeInternalLinks)
            continue;

          arrayList.add(new AnchorText(
              AnchorTextConstants.Type.INTERNAL_IN_LINK.val,
              anchor, docno));

        }
        else {
          arrayList.add(new AnchorText(
              AnchorTextConstants.Type.EXTERNAL_IN_LINK.val,
              anchor, docno));
        }

        try {
          keyWord.set(url);
          context.write(keyWord, arrayList);
        }
        catch (UTFDataFormatException e) {
          context.getCounter(LinkCounter.TEXT_TOO_LONG).increment(1);

          keyWord.set(url);
          byte flag = arrayList.get(0).getType();
          arrayList.clear();
          arrayList.add(new AnchorText(flag, AnchorTextConstants.EMPTY_STRING, docno));
          context.write(keyWord, arrayList);
        }

      }

      arrayList.clear();
      arrayList.add(new AnchorText(
          AnchorTextConstants.Type.DOCNO_FIELD.val,
          AnchorTextConstants.EMPTY_STRING, docno));
      keyWord.set(base);
      context.write(keyWord, arrayList);

      // keeping track of the number of documents that have actually been
      // processed
      context.getCounter(LinkCounter.OUTPUT_DOCS).increment(1);
    }

    private static String normalizeURL(String url) {
      try {
        URI uri = URI.create(url).normalize();
        return (new URI(uri.getScheme(), uri.getHost(), uri.getPath(), null)).toString();
      }
      catch(Exception e) {
        return null;
      }
    }
  }

  public static class Reduce extends Reducer<Text, ArrayListWritable<AnchorText>, Text, ArrayListWritable<AnchorText>> {

    private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
    private static boolean pushed;

    @Override
    public void reduce(Text key, Iterable<ArrayListWritable<AnchorText>> values, Reducer<Text, ArrayListWritable<AnchorText>, Text, ArrayListWritable<AnchorText>>.Context context) throws IOException, InterruptedException {

      arrayList.clear();

      for(ArrayListWritable<AnchorText> packet : values) {
        for (AnchorText data : packet) {

          pushed = false;

          for (int i = 0; i < arrayList.size(); i++) {
            if (arrayList.get(i).equalsIgnoreSources(data))
            {
              arrayList.get(i).addDocumentsFrom(data);
              pushed = true;
              break;
            }
          }

          if (!pushed)
            arrayList.add(data.clone());
        }
      }

      context.write(key, arrayList);
    }
  }

  public static final String[] RequiredParameters = { "Cloud9.InputPath",
    "Cloud9.OutputPath", "Cloud9.Mappers", "Cloud9.Reducers",
    "Cloud9.IncludeInternalLinks",
    "Cloud9.AnchorTextNormalizer",
    "Cloud9.DocnoMappingClass",
  "Cloud9.DocnoMappingFile" };//Modified, removed Clou9.DocnoMappingFile as required input

  public String[] getRequiredParameters() {
    return RequiredParameters;
  }

  public GenericExtractLinks(Configuration conf) {
    super(conf);
  }

  CollectionConfigurationManager configer;
  public GenericExtractLinks(Configuration conf, CollectionConfigurationManager confer) {
    super(conf);
    this.configer = confer;
  }

  @Override
  public int runTool() throws Exception {

    Configuration conf = getConf();
    Job job = new Job(conf);

    int numReducers = conf.getInt("Cloud9.Reducers", 200);

    String inputPath = conf.get("Cloud9.InputPath");
    String outputPath = conf.get("Cloud9.OutputPath");

    String mappingFile = conf.get("Cloud9.DocnoMappingFile");

    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(new Path(mappingFile))) {
      throw new RuntimeException("Error: Docno mapping data file " + mappingFile + " doesn't exist!");
    }

    DistributedCache.addCacheFile(new URI(mappingFile), conf);

    job.setJobName("ExtractLinks");
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setInt("mapred.task.timeout", 60000000);

    job.setNumReduceTasks(numReducers);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayListWritable.class);

    job.setInputFormatClass(WebDocumentInputFormat.class);
    configer.applyJobConfig(job);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

    FileInputFormat.setInputPaths(job, inputPath);
    //TODO!! changed it from sequential to FileInputFormat.. dont know whats the difference..

    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    LOG.info("ExtractLinks");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    //LOG.info(" - mapping file: " + mappingFile);
    LOG.info(" - include internal links? " + conf.getBoolean("Cloud9.IncludeInternalLinks", false));

    job.waitForCompletion(true);
    return 0;
  }
}
