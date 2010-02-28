package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ParseWikiData {
	public static final Logger sLogger = Logger.getLogger(ParseWikiData.class);


	public ParseWikiData(){

	}

	/**
	 * 
	 * 
	 * @author ferhanture
	 *
	 */
	static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, WikipediaPage, LongWritable, Text> {


		public void configure(JobConf job) {
			sLogger.setLevel(Level.DEBUG);
		}

		public void map(LongWritable key, WikipediaPage doc,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException {
			String rawtext = doc.getText();
			if(rawtext!=null){
				sLogger.debug("@RAW");
				sLogger.debug(rawtext);
				String parsed = WikipediaPage.parseAndCleanPage(rawtext);
				String[] sentences = (WikipediaPage.parseAndCleanPage(parsed)).split("[\\.\\?\\!]");
				sLogger.debug("@SENTENCES");
				int i=0;
				for(String sentence : sentences){
					sentence = sentence.trim();
					if(sentence!=null){
						/*made-up scale factor 100, to avoid two sentences have the same key. this assumes that each page will have less than 100 sentences, which works for the German wikipedia.*/
						sLogger.debug((100*key.get()+i)+":"+sentence);
						output.collect(new LongWritable(100*key.get()+i), new Text(sentence));
						i++;
					}
				}
			}
		}
	}

	public static class MyReducer extends MapReduceBase implements
	Reducer<LongWritable, Text, Text, Text> {

		public void configure(JobConf conf){

		}

		public void reduce(LongWritable sentno, Iterator<Text> text, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			int count=0;
			while(text.hasNext()){
				count++;
				if(count>1){
					throw new RuntimeException("two sentences with same id");
				}
				output.collect(new Text(""), text.next());
			}
		}

	}


	public static void main(String[] args) throws Exception {
		sLogger.setLevel(Level.DEBUG);

		sLogger.info("Parsing wiki data...");

		int mapTasks = 100;

		JobConf conf = new JobConf(ParseWikiData.class);
		conf.setJobName("ParseWikiData");
		FileSystem fs = FileSystem.get(conf);

		String collectionPath = "/umd/collections/wikipedia.raw/dewiki-20081206-pages-articles.xml";
		String outputPath = "/user/ferhan/mt/wiki.parsed2/";

		sLogger.info("Document vectors to be stored in " + outputPath);
		sLogger.info("CollectionPath: " + collectionPath);
		fs.delete(new Path(outputPath), true);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapred.map.max.attempts", 10);
		conf.setInt("mapred.reduce.max.attempts", 10);

		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		//		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

	}

}
