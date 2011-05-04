package edu.umd.hooka.alignment;

import edu.umd.hooka.alignment.model1.Model1;
import edu.umd.hooka.ttables.TTable;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import edu.umd.hooka.Alignment;
import edu.umd.hooka.PhrasePair;

/**
 * Reads a bitext and generates a TTable object (serialized) based on the
 * (e,f) cooccurrences in the text.
 * 
 * @author redpony
 *
 */
public class M1ViterbiExtract {
	
	//static final String bitext  ="/shared/bitexts/small.ar-en.ldc/ar-en.bitext";
	//static final String ttable  ="/user/redpony/small.ar-en.ttable";

	static final String bitext  ="/shared/bitexts/hansards.fr-en/hansards.aachen.bitext";
	static final String ttable  ="/user/redpony/hansards.aachen.ttable";

	static protected TTable loadTTable(Path path) throws IOException {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fileSys = FileSystem.get(conf);
	
		DataInput in = new DataInputStream(new BufferedInputStream(fileSys.open(path)));
		TTable_monolithic_IFAs tt = new TTable_monolithic_IFAs();
		tt.readFields(in);
		
		return tt;
	}
	
	public static class M1ViterbiMapper extends MapReduceBase
	  implements Mapper<IntWritable,PhrasePair,IntWritable,Text> {
		
		Text out = new Text();
		PerplexityReporter cr = new PerplexityReporter();
		Model1 m1 = null;
		public void map(IntWritable key, PhrasePair value, 
		                    OutputCollector<IntWritable,Text> output, 
		                    Reporter reporter) throws IOException {
			if (m1 == null) {
				Path pathTTable = new Path(ttable);
				TTable tt = loadTTable(pathTTable);
				m1 = new Model1(tt, true);
			}
			cr.reset();
			Alignment a = m1.viterbiAlign(value, cr);
			out.set(a.toString());
			output.collect(key, out);
			reporter.incrCounter(CrossEntropyCounters.LOGPROB, (long)(cr.getTotalLogProb()));
			reporter.incrCounter(CrossEntropyCounters.WORDCOUNT, cr.getTotalWordCount());
		}
		
		public void close() {
		}
	}
		
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		int mapTasks    = 15;
		
		JobConf conf = new JobConf(M1ViterbiMapper.class);
		conf.setJobName("m1viterbi");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(M1ViterbiMapper.class);		        
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);
		conf.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(bitext));
		FileOutputFormat.setOutputPath(conf, new Path("somealigns.test"));

		RunningJob rj = JobClient.runJob(conf);
		Counters cs = rj.getCounters();
		double lp = (double)cs.getCounter(CrossEntropyCounters.LOGPROB);
		double wc = (double)cs.getCounter(CrossEntropyCounters.WORDCOUNT);
		double ce = (lp / wc) / Math.log(2.0);
		System.out.println("Viterbi cross-entropy: " + ce + "   perplexity: " + Math.pow(2.0, ce));
	}
	
}
