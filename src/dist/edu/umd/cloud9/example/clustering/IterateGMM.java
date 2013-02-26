package edu.umd.cloud9.example.clustering;

/*
 * Cloud9: A Hadoop toolkit for working with big data
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

//package edu.umd.cloud9.example.bigram;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class IterateGMM extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IterateGMM.class);
  
  
  protected static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfStrings> {
    private static final Text comp = new Text();
    private static final PairOfStrings PairValue = new PairOfStrings();
    
    private UnivariateGaussianMixtureModel model = new UnivariateGaussianMixtureModel();
    private final Vector<String> lines=new Vector<String>();
    private double[] p;
    
    public void setup(Context context) throws IOException{
      // load the information of k clusters 
      String file = context.getConfiguration().get("clusterpath");
      FSDataInputStream cluster=FileSystem.get(context.getConfiguration()).open(new Path(file));
      BufferedReader reader = new BufferedReader(new InputStreamReader(cluster));
      lines.clear();
      while (reader.ready()){
        String line=reader.readLine();
        if (line.indexOf("lld")>=0) continue;
        if (line.length()>5)
          lines.add(line);
      }
      reader.close();
      cluster.close();

      model.setSize(lines.size());
      p= new double[model.size];
      
      for (int i=0;i<lines.size();i++){
        String[] terms = lines.elementAt(i).split("\\s+");
        int j=0;
        while (j<terms.length){
          if (terms[j].length()>0) break;
          j++;
        }
        model.pos[i]=Integer.parseInt(terms[j]);
        model.weight[i]=Double.parseDouble(terms[j+1]);
        PVector param = new PVector(2);
        param.array[0]=Double.parseDouble(terms[j+2]);
        param.array[1]=Double.parseDouble(terms[j+3]);
        model.param[i]=param;
      }
      LOG.info("setup: "+model.toString());
    }
    
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    	String line = value.toString();
    	StringTokenizer itr = new StringTokenizer(line);
    	double x=0;
    	while (itr.hasMoreTokens()){
    	  String curr = itr.nextToken();
    	  x = Double.parseDouble(curr);
    	}
    	
    	// Calculate the LogLikelihood of last iteration
    	double lld = Math.log(model.density(new Point(x)));
      comp.set("lld");
      PairValue.set(String.valueOf(x), String.valueOf(lld));
    	context.write(comp, PairValue);
    	
    	//  E step
    	 double sum = 0;
       for (int k = 0; k < model.size; k++) {
         double tmp = model.weight[k] *
             UnivariateGaussianMixtureModel.densityOfGaussian(new Point(x), model.param[k]);
         p[k] = tmp;
         sum += tmp;
       }
       for (int k = 0; k < model.size; k++) {
         p[k] /= sum;
       }
    	
    	for (int i=0;i<model.size;i++){
    	  comp.set(String.valueOf(model.pos[i]));
    	  PairValue.set(String.valueOf(x), String.valueOf(p[i]));
    	  context.write(comp, PairValue);
    	}
    }
  }



  protected static class MyReducer extends
      Reducer<Text, PairOfStrings, Text, Text> {
    private static final Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<PairOfStrings> values, Context context)
        throws IOException, InterruptedException {  
      Iterator<PairOfStrings> iter = values.iterator();

      if (key.toString().matches("lld")){
          // calculate the LogLikelihood for last iteration
          double lld=0;
          while (iter.hasNext()){
            lld+= Double.parseDouble(iter.next().getRightElement());
          }
          result.set(String.valueOf(lld));
          context.write(key, result);
      }
      else {
      
        // Variables
        double sum = 0;
        double mu = 0;
        double sigma = 0;
        double diff1 = 0;
        double diff2 = 0;
        double diff3 = 0;
        int tot=0;
        
        // First step of the computation of new mu
        while (iter.hasNext()){
          tot++;
          PairOfStrings now = iter.next();
          double w = Double.parseDouble(now.getRightElement());
          double x = Double.parseDouble(now.getLeftElement());
          sum += w;
          mu += x * w;
          diff1 += x*x*w;
          diff2 += 2*x*w;
          diff3 += w;
        }
        mu /= sum;
        sigma = (diff1 - diff2*mu + diff3*mu*mu) / sum;        
        
        double weight = sum / tot;
        
        result.set(String.valueOf(weight)+" "+String.valueOf(mu)+" "+String.valueOf(sigma));
        context.write(key, result);
      }
    }
  }

  protected static class MyPartitioner extends Partitioner<Text, PairOfStrings> {
    @Override
    public int getPartition(Text key, PairOfStrings value, int numReduceTasks) {
      return (key.toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
  
  
  public IterateGMM(){}
  
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";


  private static int printUsage() {
    System.out.println("usage: [input-path] [output-path] [num-reducers]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();
    
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath0 = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + IterateGMM.class.getSimpleName());
    LOG.info(" - input path: " + inputPath0);
    String inputPath = inputPath0+"/points";
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);


    int iterations=0;
    Configuration conf = getConf();

    while (iterations==0 || !FinishIteration(inputPath0,iterations,conf)){
      LOG.info("** iterations: "+iterations);
      try{

        Job job = Job.getInstance(conf);
        job.setJobName(IterateGMM.class.getSimpleName());
        job.setJarByClass(IterateGMM.class);
        // set the path of the information of k clusters in this iteration
        job.getConfiguration().set("clusterpath", inputPath0+"/cluster"+iterations);        
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);


        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);


        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");        
        
        reNameFile(inputPath0, outputPath,iterations+1,conf,reduceTasks);
      }catch (Exception exp){
        exp.printStackTrace(); 
      }
      iterations++;
    }
               
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new IterateGMM(),args);
  }
  
  private static final int MAX_ITERATIONS = 30;
  private static final double logLikelihoodThreshold = 10e-10;


  public static double getlld(String input, int iterations, Configuration conf){
    try{
      FSDataInputStream cluster=FileSystem.get(conf).open(new Path(input+"/cluster"+iterations));
      BufferedReader reader = new BufferedReader(new InputStreamReader(cluster));
      UnivariateGaussianMixtureModel model = new UnivariateGaussianMixtureModel();
      double lld=0;
      while (reader.ready()){
        String line=reader.readLine();
        if (line.indexOf("lld")>=0) {
          String[] terms = line.split("\\s+");
          int j=0;
          while (j<terms.length){
            if (terms[j].indexOf("lld")>=0) break;
            j++;
          }
          lld=Double.parseDouble(terms[j+1]);
          break;
        }
      }
      reader.close();
      cluster.close();
      return lld;
    } catch (IOException exp){
      exp.printStackTrace();
      return 0;
    }  
  }
    
  
  
  public static boolean FinishIteration(String input, int iterations, Configuration conf){
    if (iterations>=MAX_ITERATIONS) return true;
    if (iterations<=1) return false;
    
    double logLikelihoodNew = getlld(input, iterations, conf);
    double logLikelihoodOld = getlld(input, iterations-1, conf);

    if (Math.abs((logLikelihoodNew - logLikelihoodOld)/logLikelihoodOld) > logLikelihoodThreshold) 
      return false;
    else return true;
  }


  public static boolean reNameFile(String input, String output, int iterations, Configuration conf, int reduceTasks){
    String dstName= input+"/cluster"+iterations;
    try {
      FileSystem fs = FileSystem.get(conf);   
      fs.delete(new Path(dstName),true);
      FSDataOutputStream clusterfile=fs.create(new Path(dstName));
      
      for (int i=0;i<reduceTasks;i++){
        String srcName= output+"/part-r-"+String.format("%05d", i);
        FSDataInputStream cluster=fs.open(new Path(srcName));
        BufferedReader reader = new BufferedReader(new InputStreamReader(cluster));
        while (reader.ready()){
          String line=reader.readLine()+"\n";
          if (line.length()>5)
          clusterfile.write(line.getBytes());
        }
        reader.close();
        cluster.close(); 
      }
      clusterfile.flush();
      clusterfile.close();
      return true;
    }catch (IOException e){
      e.printStackTrace();
      return false;
    }
  }
}
