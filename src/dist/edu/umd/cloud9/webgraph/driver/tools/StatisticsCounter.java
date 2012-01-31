package edu.umd.cloud9.webgraph.driver.tools;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import java.io.File;
import org.apache.hadoop.util.ToolRunner;


public class StatisticsCounter extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
	int res = ToolRunner
	        .run(new Configuration(), new StatisticsCounter(), args);

	System.exit(res);
    }

    Configuration conf;

    String inputBase;
    String outputBase;
    
    public static final String outputIDFCounts = "InverseDocumentFrequency";
    public int run(String[] arg0) throws Exception
    {
	conf = new Configuration();
	if (!readInput(arg0))
	{
	    printUsage();
	    return -1;
	}
	
	String inputPath = inputBase;
	String outputPath = outputBase + "/" + outputIDFCounts;

	conf.set("Cloud9.InputPath", inputPath);
	conf.set("Cloud9.OutputPath", outputPath);
	conf.setInt("Cloud9.Mappers", 1);
	conf.setInt("Cloud9.Reducers", 200);
	
	int r = new IDFCounter(conf).run();

	if (r != 0)
	{
		return -1;
	}
	// TODO Auto-generated method stub
	return 0;
    }

    private static int printUsage()
    {
	System.out.println("\nusage: input-path output-path");
	return -1;
    }

    private boolean readInput(String[] args)
    {
	if (args.length < 2)
	{
	    printUsage();
	    System.out.println("More arguments needed.");
	    return false;
	}
	inputBase = new File(args[0]).getAbsolutePath();
	outputBase = new File(args[1]).getAbsolutePath();
	return true;
    }
}
