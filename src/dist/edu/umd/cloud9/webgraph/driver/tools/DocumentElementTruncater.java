package edu.umd.cloud9.webgraph.driver.tools;

/*
 * CodumentElementTruncater
 * a tool designed to truncate target MarkableLanguage document into sections
 * according to specified tag element
 * 
 * Author OceanMaster
 */

/*
 * reform:
 * making output value WebDocument instead of Text.
 * more work needed to make it generic
 */


import java.io.IOException;
import java.io.UTFDataFormatException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.Tag;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.tags.BaseHrefTag;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.tags.TitleTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.generic.WebDocument;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trecweb.TrecWebDocument;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph.Map;
import edu.umd.cloud9.webgraph.BuildWebGraph.Reduce;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;
import edu.umd.cloud9.webgraph.driver.wt10g.CollectionConfigurationManager;
import edu.umd.cloud9.webgraph.driver.wt10g.GenericExtractLinks;
import edu.umd.cloud9.webgraph.driver.wt10g.GenericExtractLinks.Map.LinkCounter;
import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

public class DocumentElementTruncater extends PowerTool
{
    private static final Logger LOG = Logger
	    .getLogger(DocumentElementTruncater.class);
    SimpleConfigurationManager configer;

    public static class Map extends
	    Mapper<LongWritable, WebDocument, LongWritable, WebDocument>
    {
	public static class HeadingTagFilter implements NodeFilter
	{
	    Pattern pattern = Pattern.compile("h[123456]",
		    Pattern.CASE_INSENSITIVE);

	    // it will take all tags with the name like h1
	    public boolean accept(Node node)
	    {
		return (pattern.matcher(node.getText()).matches());
	    }

	}

	private static String tag;

	// private static String base; // base URL for current document
	// private static String baseHost;
	private static int docno; // docno of current document

	// private static final Text keyWord = new Text(); // output key for the
	// mappers
	// private static final ArrayListWritable<AnchorText> arrayList = new
	// ArrayListWritable<AnchorText>();
	// output value for the mappers

	// private static DocnoMapping docnoMapping = null;

	private static final Parser parser = new Parser();
	private static NodeFilter filter;
	
	private static final LongWritable myKey = new LongWritable();
	private static final Text docid = new Text();
	private static final Text contentText = new Text();

	// private static final NodeFilter filter = new
	// NodeClassFilter(LinkTag.class);
	// private static NodeList list;

	// private static AnchorTextNormalizer normalizer;

	@Override
	public void setup(
	        Mapper<LongWritable, WebDocument, LongWritable, WebDocument>.Context context)
	        throws IOException
	{
	    Configuration conf = context.getConfiguration();
	    tag = conf.get("Cloud9.targetTag");

	    if (tag.equalsIgnoreCase("heading"))
		filter = new HeadingTagFilter();
	    else
		filter = new TagNameFilter(tag);

	    // String docnoMappingClass = conf.get("Cloud9.DocnoMappingClass");
	    // try
	    // {
	    // docnoMapping = (DocnoMapping) Class.forName(docnoMappingClass)
	    // .newInstance();
	    // }
	    // catch (Exception e)
	    // {
	    // throw new RuntimeException(
	    // "Error initializing DocnoMapping class!");
	    // }
	    //
	    // String docnoMappingFile = conf.get("Cloud9.DocnoMappingFile",
	    // null);
	    // if (docnoMappingFile != null)
	    // {
	    // Path docnoMappingPath = null;
	    // try
	    // {
	    // Path[] localFiles = DistributedCache
	    // .getLocalCacheFiles(conf);
	    // if (localFiles != null)
	    // {
	    // docnoMappingPath = localFiles[0];
	    // }
	    // else
	    // {
	    // docnoMappingPath = new Path(
	    // conf.get("Cloud9.DocnoMappingFile"));
	    // }
	    // }
	    // catch (IOException e)
	    // {
	    // throw new RuntimeException(
	    // "Unable to find DocnoMappingFile!");
	    // }
	    //
	    // try
	    // {
	    // docnoMapping.loadMapping(docnoMappingPath,
	    // FileSystem.getLocal(conf));
	    // }
	    // catch (Exception e)
	    // {
	    // e.printStackTrace();
	    // throw new RuntimeException(
	    // "Error initializing DocnoMapping!");
	    // }
	    // }

	    // try
	    // {
	    // normalizer = (AnchorTextNormalizer) Class.forName(
	    // conf.get("Cloud9.AnchorTextNormalizer")).newInstance();
	    // }
	    // catch (Exception e)
	    // {
	    // e.printStackTrace();
	    // throw new RuntimeException(
	    // "Error initializing AnchorTextNormalizer");
	    // }
	}

	@Override
	public void map(LongWritable key, WebDocument doc,
	        Mapper<LongWritable, WebDocument, LongWritable, WebDocument>.Context context)
	        throws IOException, InterruptedException
	{
	    context.getCounter(LinkCounter.INPUT_DOCS).increment(1);
	    myKey.set(key.get());
	    docid.set(doc.getDocid());

	    // try
	    // {
	    // docno = docnoMapping.getDocno(doc.getDocid());
	    // }
	    // catch (NullPointerException e)
	    // {
	    // // Discard documents with an invalid document number
	    // context.getCounter(LinkCounter.INVALID_DOCNO).increment(1);
	    // return;
	    // }

	    // try
	    // {
	    // base = normalizeURL(doc.getURL());
	    // }
	    // catch (Exception e)
	    // {
	    // // Discard documents with which there is no URL associated
	    // context.getCounter(LinkCounter.INVALID_URL).increment(1);
	    // return;
	    // }
	    //
	    // if (base == null)
	    // {
	    // context.getCounter(LinkCounter.INVALID_URL).increment(1);
	    // return;
	    // }
	    //
	    // try
	    // {
	    // baseHost = new URI(base).getHost();
	    // }
	    // catch (Exception e)
	    // {
	    // context.getCounter(LinkCounter.INVALID_URL).increment(1);
	    // return;
	    // }
	    //
	    // if (baseHost == null)
	    // {
	    // context.getCounter(LinkCounter.INVALID_URL).increment(1);
	    // return;
	    // }
	    NodeList nl;
	    try
	    {
		parser.setInputHTML(doc.getContent()); // initializing the
		// parser with new HTML
		// content

		// Setting base URL for the current document
		nl = parser.parse(filter);

		// Tag html = new TitleTag();
		// Tag end = new TitleTag();
		// html.setTagName("html");
		// end.setTagName("/html");
		// html.setEndTag(end);
		// html.setChildren(nl);

		// BaseHrefTag baseTag = new BaseHrefTag();
		// baseTag.setBaseUrl(base);
		// nl.add(baseTag);

		// re-initializing the parser with the fixed content
		// parser.setInputHTML(nl.toHtml());

		// listing all LinkTag nodes
		// list = parser.extractAllNodesThatMatch(filter);
	    }
	    catch (ParserException e)
	    {
		context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
		return;
	    }
	    catch (StackOverflowError e)
	    {
		context.getCounter(LinkCounter.PARSER_FAILED).increment(1);
		return;
	    }

	    String content = "<DOC>\n<DOCNO>" + doc.getDocid()
		    + "</DOCNO>\n<HTML>\n";
	    for (int i = 0; i < nl.size(); i++)
	    {
		content += nl.elementAt(i).toHtml() + "\n";
	    }
	    content += "</HTML>\n</DOC>\n";

	    contentText.set(content);
	    
	    //TODO
	    //potential bug..need confirm with Nima
	    doc.setDocumentContent(content);
	    
	    context.write(myKey, doc);
	    // keeping track of the number of documents that have actually been
	    // processed
	    context.getCounter(LinkCounter.OUTPUT_DOCS).increment(1);
	}
    }

    public static class Reduce extends
	    Reducer<LongWritable, WebDocument, LongWritable, WebDocument>
    {

    }

    public static final String[] RequiredParameters = { "Cloud9.InputPath",
	    "Cloud9.OutputPath", "Cloud9.Mappers", "Cloud9.Reducers",
	    "Cloud9.targetTag" };

    public String[] getRequiredParameters()
    {
	return RequiredParameters;
    }

    public DocumentElementTruncater(Configuration conf)
    {
	super(conf);
    }

    public DocumentElementTruncater(Configuration conf,
	    SimpleConfigurationManager configer)
    {
	super(conf);
	this.configer = configer;
    }

    @Override
    public int runTool() throws Exception
    {
	Configuration conf = getConf();
	Job job = new Job(conf);

	int numMappers = conf.getInt("Cloud9.Mappers", 1);
	int numReducers = conf.getInt("Cloud9.Reducers", 200);
	String inputPath = conf.get("Cloud9.InputPath");
	String outputPath = conf.get("Cloud9.OutputPath");
	// String mappingFile = conf.get("Cloud9.DocnoMappingFile");
	String tag = conf.get("Cloud9.targetTag");

	FileSystem fs = FileSystem.get(conf);
	// if (!fs.exists(new Path(mappingFile)))
	// {
	// throw new RuntimeException("Error: Docno mapping data file "
	// + mappingFile + " doesn't exist!");
	// }
	// DistributedCache.addCacheFile(new Path(mappingFile).toUri(),
	// job.getConfiguration());

	job.setJobName("TruncateDocumentByElement - " + tag);
	conf.set("mapred.child.java.opts", "-Xmx4096m");
	conf.setInt("mapred.task.timeout", 60000000);

	job.setNumReduceTasks(numReducers);

	job.setMapperClass(Map.class);
	// job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);

	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(WebDocument.class);

	configer.applyJobConfig(job);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	SequenceFileOutputFormat.setCompressOutput(job, true);
	SequenceFileOutputFormat.setOutputCompressionType(job,
	        SequenceFile.CompressionType.BLOCK);

	recursivelyAddInputPaths(job, inputPath);
	// FileInputFormat.addInputPaths(job, inputPath);
	FileOutputFormat.setOutputPath(job, new Path(outputPath));

	LOG.info("TruncateDocumentByElement - " + tag);
	LOG.info(" - input path: " + inputPath);
	LOG.info(" - output path: " + outputPath);
	LOG.info(" - tag: " + tag);

	// FileSystem fs = FileSystem.get(conf);
	// if (!fs.exists(new Path(outputPath)))
	// {
	// JobClient.runJob(conf);
	// }
	// else
	// {
	// LOG.info(outputPath +
	// "output path already exists! Skipping this step...");
	// }

	job.waitForCompletion(true);
	return 0;

    }

    public static void recursivelyAddInputPaths(Job job, String path)
	    throws IOException
    {
	FileSystem fs;
	try
	{
	    fs = FileSystem.get(new URI(path), job.getConfiguration());
	}
	catch (URISyntaxException e)
	{
	    throw new RuntimeException("Error recursively adding path -- "
		    + path);
	}

	FileStatus[] ls = fs.listStatus(new Path(path));
	for (FileStatus status : ls)
	{
	    // skip anything that starts with an underscore, as it often
	    // indicates
	    // a log directory or another special type of Hadoop file
	    if (status.getPath().getName().startsWith("_"))
	    {
		continue;
	    }

	    if (status.isDir())
	    {
		recursivelyAddInputPaths(job, status.getPath().toString());
	    }
	    else
	    {
		FileInputFormat.addInputPath(job, status.getPath());
	    }
	}
    }
}
