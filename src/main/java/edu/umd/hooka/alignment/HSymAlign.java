package edu.umd.hooka.alignment;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.hooka.Alignment;

public class HSymAlign {

	public static class MapClass extends MapReduceBase
	  implements Mapper<LongWritable,Text,IntWritable,Text> {
		    
		private Text l = new Text();
		private IntWritable linenum = new IntWritable(1); 
		    
		public void map(LongWritable key, Text value, 
				OutputCollector<IntWritable,Text> output, 
				Reporter reporter) throws IOException {
			String line = value.toString();
			if (line.length() == 0) { return; }
			String[] toks = line.split("\\s*\\|\\|\\|\\s*");
			if (toks.length != 2)
				throw new IOException("Expected input of form '0 ||| /path/to/input'");
			String pfx = toks[0];
			if (pfx.length() != 1 && (!pfx.equals("0") || !pfx.equals("1")))
				throw new IOException("Excepted transpose field to be 0 or 1");
			Path p = new Path(toks[1]);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			FileSystem fileSys = FileSystem.get(conf);
			BufferedReader giza = new BufferedReader(new InputStreamReader(fileSys.open(p), "UTF8"));
			int lc = 0;
			String comment;
			while ((comment = giza.readLine()) != null) {
				String e = giza.readLine();
				String f = giza.readLine();
				lc++;
				linenum.set(lc);
		        l.set(pfx + " ||| " + comment + " ||| " + e + " ||| " + f);
		        output.collect(linenum, l);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase
	  implements Reducer<IntWritable,Text,IntWritable,Text> {
		    
		Text alout = new Text();
		Refiner r = null;
		
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable,Text> output, 
				Reporter reporter) throws IOException {
			if (r == null) {
				try {
					r = RefinerFactory.getForName("grow-diag-final-and");
				} catch (Exception e) {
					throw new IOException("Caught exception: " + e);
				}
			}
			Text ta = values.next();
			Text tb = values.next();
			if (ta == null || tb == null) { throw new IOException("Layout error!"); }
			String sa = ta.toString();
			String sb = tb.toString();
			String e2f = sa;
			String f2e = sa;
			if (sb.charAt(0) == '0') { f2e = sb; } else { e2f = sb; }
			String[] ae2f = e2f.split("\\s*\\|\\|\\|\\s*");
			String[] af2e = f2e.split("\\s*\\|\\|\\|\\s*");
			Alignment a1 = Alignment.fromGiza(ae2f[1], ae2f[2], true);
			Alignment a2 = Alignment.fromGiza(af2e[1], af2e[2], false);
			Alignment a = r.refine(a1, a2);
			alout.set(a.toString());
			output.collect(key, alout);
		}
	}
	
	public static void main(String[] args) {
		JobConf conf = new JobConf(HSymAlign.class);
		conf.setJobName("alignment-sym");
		
		conf.setOutputKeyClass(IntWritable.class);            // the keys are words (strings)
		conf.setOutputValueClass(Text.class);   // the values are counts (ints)
		    
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		        
		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(500);
		String filename="infiles";
		String outputPath="align";
		FileInputFormat.setInputPaths(conf, new Path(filename));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
			        
		try{
		    JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
