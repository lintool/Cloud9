/**
 * 
 */
package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sun.awt.SunHints.Value;

import edu.umd.cloud9.io.PairOfStrings;
import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;

/**
 * @author michaelmcgrath
 *
 */
public class HubsAndAuthoritiesInit extends Configured implements Tool {

	private static final Schema MAP_SCHEMA = new Schema();
	private static final Schema VAL_SCHEMA = new Schema();
	private static final Schema PAYLOAD_SCHEMA = new Schema();
	
	static {
		MAP_SCHEMA.addField("link", String.class);
		MAP_SCHEMA.addField("rank", Double.class);
		VAL_SCHEMA.addField("rankType", String.class);
		VAL_SCHEMA.addField("payload", Tuple.class);
		PAYLOAD_SCHEMA.addField("rank", DoubleWritable.class);
		PAYLOAD_SCHEMA.addField("adjList", ArrayListWritable.class);
	}
	
	private static final Logger sLogger = Logger.getLogger(HubsAndAuthoritiesInit.class);
	
	/**
	 * @param args
	 */
	private static class HAMapper extends MapReduceBase implements
	Mapper<Text, Tuple, Text, Tuple>
	{
		//private Tuple valIn = MAP_SCHEMA.instantiate();
		private Tuple payloadIn = PAYLOAD_SCHEMA.instantiate();
		private Tuple valOut = VAL_SCHEMA.instantiate();
		private Tuple payloadOut = PAYLOAD_SCHEMA.instantiate();
		private Text keyOut = new Text();
		private ArrayListWritable<Text> empty = new ArrayListWritable<Text>();
		private final static DoubleWritable dummy = new DoubleWritable(-1.0);
	
		public void map(Text key, Tuple value,
				OutputCollector<Text, Tuple> output, Reporter reporter) throws IOException {
		
			String type = value.getSymbol("rankType");
			payloadIn = (Tuple) value.get("payload");
			ArrayListWritable<Text> adjList = (ArrayListWritable<Text>) payloadIn.get("adjList");
			valOut.setSymbol("rankType", type);
			payloadOut.set("rank", dummy);
			payloadOut.set("adjList", adjList);
			valOut.set("payload", payloadOut);
			
			//System.out.println(key.toString() + " " + valOut.toString());
			output.collect(key, valOut);
			
			valOut.setSymbol("rankType", type);
			payloadOut.set("rank", (DoubleWritable) payloadIn.get("rank"));
			payloadOut.set("adjList", empty);
			valOut.set("payload", payloadOut);
			
			output.collect(key, valOut);
			
			Iterator<Text> itr = adjList.iterator();
			Text curr;
			
			String symbolOut;
			
			if (value.getSymbol("rankType").equals("H"))
			{
				symbolOut = "A";
			}
			else
			{
				symbolOut = "H";
			}
			
			while( itr.hasNext() )
			{
				curr = itr.next();
				valOut.setSymbol("rankType", symbolOut);
				payloadOut.set("rank", (DoubleWritable) payloadIn.get("rank"));
				payloadOut.set("adjList", empty);
				valOut.set("payload", payloadOut);
				
				output.collect(curr, valOut);
			}	
		}
		
	}
	
	private static class HAReducer extends MapReduceBase implements
	Reducer<Text, Tuple, Text, Tuple>
	{
		private Tuple valIn = VAL_SCHEMA.instantiate();
		private Tuple payloadIn = PAYLOAD_SCHEMA.instantiate();
		private Tuple hvalOut = VAL_SCHEMA.instantiate();
		private Tuple avalOut = VAL_SCHEMA.instantiate();
		private Tuple hpayloadOut = PAYLOAD_SCHEMA.instantiate();
		private Tuple apayloadOut = PAYLOAD_SCHEMA.instantiate();
		
		private ArrayListWritable<Text> emptyList = new ArrayListWritable<Text>();
		public void reduce(Text key, Iterator<Tuple> values, OutputCollector<Text, Tuple> output,
				Reporter reporter) throws IOException
		{
			ArrayListWritable<Text> adjList = new ArrayListWritable<Text>();
			//ArrayListWritable<Text> adjListOut = new ArrayListWritable<Text>();
			double hrank = 0;
			double arank = 0;
			//DoubleWritable rankIn;
			
			hpayloadOut.set("adjList", adjList);
			apayloadOut.set("adjList", adjList);
			
			while (values.hasNext())
			{
				valIn = (Tuple) values.next();
				//System.out.println(key.toString() + " " + valIn.toString());
				//get type
				String type = valIn.getSymbol("rankType");
				adjList.clear();
				//System.out.println(key.toString() + " " + type);
				//get payload
				payloadIn = (Tuple) valIn.get("payload");
				DoubleWritable rankIn = (DoubleWritable) payloadIn.get("rank");
				//System.out.println(key.toString() + " " + type + " " + rankIn.toString());
				//if rank == -1, get adj & store
				if ( rankIn.get() < 0.0)
				{
					adjList = (ArrayListWritable<Text>) payloadIn.get("adjList");
					//System.out.println(key.toString() + " " + type + " " + adjList.toString());
					if (type.equals("H"))
					{
						//System.out.println("H> " + key.toString() + " " + type + " " + adjList.toString());
						hpayloadOut.set("adjList", new ArrayListWritable<Text>(adjList));
						//System.out.println(key.toString() + " " + "H" + " " + hpayloadOut.toString());
					}
					else
					{
						//System.out.println("A> " + key.toString() + " " + type + " " + adjList.toString());
						apayloadOut.set("adjList", new ArrayListWritable<Text>(adjList));
						//System.out.println(key.toString() + " " + "A" + " " + hpayloadOut.toString());
					}
				}
				//else add rank to current rank
				else
				{
					if (type.equals("H"))
					{
						hrank += rankIn.get();
					}
					else
					{
						arank += rankIn.get();
					}
				}
			}
			//System.out.println(key.toString() + " " + "H" + " " + hpayloadOut.toString());
			/*if (hrank < 0.001)
			{
				hrank += 1.0;
			}
			if (arank < 0.001)
			{
				arank += 1.0;
			}*/
			///fixmelater
			//hrank = 1.0;
			//arank = 1.0;
			//build output tuple and write to output
			hpayloadOut.set("rank", new DoubleWritable(hrank));
			apayloadOut.set("rank", new DoubleWritable(arank));
			hvalOut.setSymbol("rankType", "H");
			avalOut.setSymbol("rankType", "A");
			hvalOut.set("payload", hpayloadOut);
			avalOut.set("payload", apayloadOut);
			
			output.collect(key, hvalOut);
			output.collect(key, avalOut);
		}
	}
	
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: HubsAndAuthorities");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(HubsAndAuthoritiesInit.class);
		conf.setJobName("HubsAndAuthoritiesInit");
		
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileInputFormat.addInputPath(conf, new Path("/tmp/ccc_mmcgrath/webgraphb"));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Tuple.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(HAMapper.class);;
		conf.setReducerClass(HAReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);
		
		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HubsAndAuthoritiesInit(), args);
		System.exit(res);
	}

}
