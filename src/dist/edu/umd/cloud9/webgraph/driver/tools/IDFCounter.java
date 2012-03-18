package edu.umd.cloud9.webgraph.driver.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.PowerTool;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph.Map;
import edu.umd.cloud9.webgraph.BuildWebGraph.Reduce;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.AnchorTextConstants;

public class IDFCounter extends PowerTool
{
    private static final Logger LOG = Logger.getLogger(BuildWebGraph.class);

    public static String regular(String in)
    {
	return in.trim().toLowerCase();
    }
    
    
    public static class Map extends MapReduceBase
	    implements
	    Mapper<IntWritable, ArrayListWritable<AnchorText>, Text, IntWritable>
    {

	//private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
	private static final IntWritable keyWord = new IntWritable();
	private static String ToTalNumName = "_#_TotalDocumentNumber_#_";
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	private static byte flag;

	public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
	        OutputCollector<Text, IntWritable> output, Reporter reporter)
	        throws IOException
	{
	    //1: calculate total number
	    word.set(ToTalNumName);
	    output.collect(word, one);
	    
	    //2: calculater IDF
	    HashMap<String,Boolean> appeared = new HashMap<String,Boolean>();
	    
	    for (AnchorText data : anchors)
	    {
		if(data.isDocnoField()||data.isInDegree()||data.isOfOtherTypes()||data.isOutDegree()||data.isURL())
		    continue;
		
		String anchorText = data.getText();
		//skip empty anchor text
		if(anchorText == null || anchorText.isEmpty() || anchorText.equals(AnchorTextConstants.EMPTY_STRING))
		    continue;
		// regularize
		anchorText = regular(anchorText);
		
		String[] anchorTextPieces = anchorText.split("[\t ]+");
		for(String text : anchorTextPieces)
		{
		    text = text.trim();
		    if(text.isEmpty() || appeared.containsKey(text))
			continue;
		    
		    word.set(text);
		    output.collect(word, one);
		    appeared.put(text, true);
		}
	    }

	}
    }

    public static class Reduce extends MapReduceBase implements
	    Reducer<Text, IntWritable, Text, IntWritable>
    {

	//private static final ArrayListWritable<AnchorText> arrayList = new ArrayListWritable<AnchorText>();
	//private static ArrayListWritable<AnchorText> packet;
	//private static boolean pushed;
	//private static int outdegree;

	public void reduce(
		Text key,
		Iterator<IntWritable> values,
	        OutputCollector<Text, IntWritable> output,
	        Reporter reporter) throws IOException
	{
	    int sum = 0;
	    while(values.hasNext())
	    {
		sum+=values.next().get();
	    }
	    output.collect(key, new IntWritable(sum));
	}
    }

    public static final String[] RequiredParameters = { "Cloud9.InputPath",
	    "Cloud9.OutputPath", "Cloud9.Mappers", "Cloud9.Reducers" };

    public String[] getRequiredParameters()
    {
	return RequiredParameters;
    }

    public IDFCounter(Configuration conf)
    {
	super(conf);
    }

    @Override
    public int runTool() throws Exception
    {
	JobConf conf = new JobConf(getConf(), IDFCounter.class);
	FileSystem fs = FileSystem.get(conf);

	int numMappers = conf.getInt("Cloud9.Mappers", 1);
	int numReducers = conf.getInt("Cloud9.Reducers", 200);

	String inputPath = conf.get("Cloud9.InputPath");
	String outputPath = conf.get("Cloud9.OutputPath");

	conf.setJobName("CountInverseDocumentFrequency");
	conf.set("mapred.child.java.opts", "-Xmx4096m");
	conf.setInt("mapred.task.timeout", 60000000);

	conf.setNumMapTasks(numMappers);
	conf.setNumReduceTasks(numReducers);

	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	// conf.setMapOutputKeyClass(Text.class);
	// conf.setMapOutputValueClass(IntWritable.class);

	conf.setInputFormat(SequenceFileInputFormat.class);
	conf.setOutputFormat(SequenceFileOutputFormat.class);
	// conf.setOutputFormat(TextOutputFormat.class);

	// SequenceFileOutputFormat.setCompressOutput(conf, true);
	// SequenceFileOutputFormat.setOutputCompressionType(conf,
	// SequenceFile.CompressionType.BLOCK);

	// FileInputFormat.addInputPaths(conf, inputPath);
	SequenceFileInputFormat.setInputPaths(conf, inputPath);
	FileOutputFormat.setOutputPath(conf, new Path(outputPath));

	LOG.info("CountInverseDocumentFrequency");
	LOG.info(" - input path: " + inputPath);
	LOG.info(" - output path: " + outputPath);

	if (!fs.exists(new Path(outputPath)))
	{
	    JobClient.runJob(conf);
	}
	else
	{
	    LOG.info(outputPath + " already exists! Skipping this step...");
	}

	return 0;
    }

}
