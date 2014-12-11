package edu.umd.hooka;

import edu.umd.hooka.alignment.IndexedFloatArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
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


public class PhraseExtractAndCount_stripe {
	public static class PhrasePairExtractMapper extends MapReduceBase
	  implements Mapper<IntWritable, PhrasePair, Phrase, Phrase2CountMap>
	{
		    
		private final Phrase2CountMap pcm = new Phrase2CountMap();
		private final static FloatWritable one = new FloatWritable(1.0f);
		public void map(IntWritable key, PhrasePair value, 
				OutputCollector<Phrase, Phrase2CountMap> output, 
				Reporter reporter) throws IOException {
			ArrayList<PhrasePair> extracts = value.extractConsistentPhrasePairs(7);
			for (PhrasePair p : extracts) {
				pcm.clear();
				pcm.put(p.getF(), one);
				output.collect(p.getE(), pcm);
				
				pcm.clear();
				pcm.put(p.getE(), one);
				output.collect(p.getF(), pcm);
		    }
		}
	}

	public static class PPCountCombiner extends MapReduceBase
	  implements Reducer<Phrase, Phrase2CountMap, Phrase, Phrase2CountMap> {
		Phrase2CountMap sum = new Phrase2CountMap();
		public void reduce(Phrase key, Iterator<Phrase2CountMap> values,
				OutputCollector<Phrase,Phrase2CountMap> output, 
				Reporter reporter) throws IOException {
			sum.clear();
			while (values.hasNext()) {
				sum.plusEquals(values.next());
			}
			output.collect(key, sum);
		}
	}

	public static class PPNormalizingReducer extends MapReduceBase
	  implements Reducer<Phrase, Phrase2CountMap, PhrasePair, IndexedFloatArray> {
		Phrase2CountMap sum = new Phrase2CountMap();
		PhrasePair ko = new PhrasePair();
		IndexedFloatArray scores = new IndexedFloatArray(2);
		public void reduce(Phrase key, Iterator<Phrase2CountMap> values,
				OutputCollector<PhrasePair,IndexedFloatArray> output, 
				Reporter reporter) throws IOException {
			sum.clear();
			int sc = 0;
			while (values.hasNext()) {
				sc++;
				if (sc % 1000 == 0) { reporter.progress(); }
				sum.plusEquals(values.next());
			}
			sum.normalize();
			boolean transpose = (key.getLanguage() == 0);
			if (transpose)
				ko.setE(key);
			else
				ko.setF(key);
			for (Map.Entry<Phrase,FloatWritable> i : sum.entrySet()) {
				scores.clear();
				if (transpose) {
					ko.setF(i.getKey());
					scores.set(1, i.getValue().get());
				} else {
					ko.setE(i.getKey());
					scores.set(0, i.getValue().get());
				}
				output.collect(ko, scores);
			}
		}
	}
	
	public static class ReduceSumScores extends MapReduceBase
		  	implements Reducer<PhrasePair, IndexedFloatArray, PhrasePair, IndexedFloatArray> {
			    
		IndexedFloatArray scores = new IndexedFloatArray(2);
			  
		public void reduce(PhrasePair key, Iterator<IndexedFloatArray> values,
				OutputCollector<PhrasePair, IndexedFloatArray> output, 
				Reporter reporter) throws IOException {
			scores.clear();
			while (values.hasNext()) {
				scores.plusEquals(values.next());
			}
			output.collect(key, scores);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		CorpusInfo corpus =
			CorpusInfo.getCorpus(CorpusInfo.Corpus.ARABIC_5000k);
		Path ppCountTemp= new Path("ppc.phase1.tmp");
		int mapTasks    = 38;
		int reduceTasks = 38;
			
		JobConf conf = new JobConf(PhraseExtractAndCount_stripe.class);

		conf.setJobName("BuildPT.ExtractAndCount_striped");

		FileSystem.get(conf).delete(ppCountTemp);
		FileSystem.get(conf).delete(corpus.getLocalPhraseTable());
		 
		conf.setOutputKeyClass(PhrasePair.class);
		conf.setOutputValueClass(IndexedFloatArray.class);
		conf.setMapOutputKeyClass(Phrase.class);
		conf.setMapOutputValueClass(Phrase2CountMap.class);
		
		conf.setMapperClass(PhrasePairExtractMapper.class);        
		conf.setCombinerClass(PPCountCombiner.class);
		conf.setReducerClass(PPNormalizingReducer.class);
		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, corpus.getAlignedBitext());
		FileOutputFormat.setOutputPath(conf, ppCountTemp);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		        
		JobClient.runJob(conf);

		conf = new JobConf(PhraseExtractAndCount_stripe.class);
		conf.setJobName("BuildPT.Merge");
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setOutputKeyClass(PhrasePair.class);
		conf.setOutputValueClass(IndexedFloatArray.class);
		
		conf.setReducerClass(ReduceSumScores.class);
		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		FileInputFormat.setInputPaths(conf, ppCountTemp);
		FileOutputFormat.setOutputPath(conf, corpus.getLocalPhraseTable());

		JobClient.runJob(conf);

	}	  
}
