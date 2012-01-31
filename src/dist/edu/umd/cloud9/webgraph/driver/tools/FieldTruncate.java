package edu.umd.cloud9.webgraph.driver.tools;

import java.io.File;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

public class FieldTruncate extends Configured implements Tool
{
    Configuration conf;
    String inputBase;
    String outputBase;
    Vector<String> elementTags = new Vector<String>();
    SimpleConfigurationManager configer;
    private String normalizer = "edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer";

    public static void main(String[] args) throws Exception
    {
	int res = ToolRunner.run(new Configuration(), new FieldTruncate(),
	        args);

	System.exit(res);
    }

    private static int printUsage()
    {
	System.out
	        .println("\nUsage: input-path output-path -cn {clueweb|trec|sequence} -et TargetElemtntTag [-et TargetElemtntTag [..]] [-ci userSpecifiedInputFormatClass] -cf userSpecifiedDocnoMappingFile [-cm userSpecifiedDocnoMappingClass]");
	System.out
	        .println(" Help: -cn {clueweb|trec|sequence_file}                     specify file format, clueweb, trec or sequence files;");
	System.out
	        .println("     : -et TargetElemtntTag [-et TargetElemtntTag [..]]     specify tags of elements that should be reserved in truncated file");
	System.out
	        .println("     : [-ci userSpecifiedInputFormatClass]                  specify InputFormat class");
//	System.out
//	        .println("     : [-cm userSpecifiedDocnoMappingClass]                 specify DocnoMapping class");
//	System.out
//	        .println("     : -cf userSpecifiedDocnoMappingFile                    specify DocnoMapping file");
//	System.out
//	        .println("     : [-nm normalizerClass]                                specify Normalizer class");

	return -1;
    }

    private boolean readInput(String[] args)
    {
	if (args.length < 6)
	{
	    System.out.println("More arguments needed.");
	    return false;
	}
	inputBase = new File(args[0]).getAbsolutePath();
	outputBase = new File(args[1]).getAbsolutePath();

	int argc = args.length - 2;
	while (argc > 0)
	{
	    String cmd = args[args.length - argc];

	    if (cmd.equals("-cn"))
	    {
		if (argc < 2)
		{
		    System.out
			    .println("Insufficient arguments, more arguments needed after -cn flag.");
		    return false;
		}
		argc--;
		String collectionName = args[args.length - argc];
		if (!configer.setConfByCollection(collectionName))
		{
		    System.out
			    .println("Collection \""
			            + collectionName
			            + "\" not supported, please specify inputformat and docnomapping class, or contact developer.");
		    return false;
		}

	    }
	    else if (cmd.equals("-et"))
	    {
		if (argc < 2)
		{
		    System.out
			    .println("Insufficient arguments, more arguments needed after -et flag.");
		    return false;
		}
		argc--;
		String tagName = args[args.length - argc];
		elementTags.add(tagName);
	    }
	    else if (cmd.equals("-ci"))
	    {
		if (argc < 2)
		{
		    System.out
			    .println("Insufficient arguments, more arguments needed after -ci flag.");
		    return false;
		}
		argc--;
		String ciName = args[args.length - argc];
		if (!configer.setUserSpecifiedInputFormat(ciName))
		{
		    System.out
			    .println("class \""
			            + ciName
			            + "\" doesn't exist or not sub-class of FileInputFormat");
		    return false;
		}
	    }
//	    else if (cmd.equals("-cm"))
//	    {
//		if (argc < 2)
//		{
//		    System.out
//			    .println("Insufficient arguments, more arguments needed after -cm flag.");
//		    return false;
//		}
//		argc--;
//		String cmName = args[args.length - argc];
//
//		if (!configer.setUserSpecifiedDocnoMappingClass(cmName))
//		{
//		    System.out
//			    .println("class \""
//			            + cmName
//			            + "\" doesn't exist or not implemented DocnoMappingt");
//		    return false;
//		}
//		// conf.set("Cloud9.DocnoMappingClass", cmName);
//	    }
//	    else if (cmd.equals("-cf"))
//	    {
//		if (argc < 2)
//		{
//		    System.out
//			    .println("Insufficient arguments, more arguments needed after -cm flag.");
//		    return false;
//		}
//		argc--;
//		String cfName = args[args.length - argc];
//		conf.set("Cloud9.DocnoMappingFile", cfName);
//	    }
//	    else if (cmd.equals("-nm"))
//	    {
//		if (argc < 2)
//		{
//		    System.out
//			    .println("Insufficient arguments, more arguments needed after -nm flag.");
//		    return false;
//		}
//		argc--;
//		String nm = args[args.length - argc];
//		;
//		try
//		{
//		    if (!AnchorTextNormalizer.class.isAssignableFrom(Class
//			    .forName(nm)))
//		    {
//			System.out
//			        .println("Invalid arguments; Normalizer class must implement AnchorTextNormalizer interface.");
//			return false;
//		    }
//		}
//		catch (ClassNotFoundException e)
//		{
//		    System.out
//			    .println("Invalid arguments; Specified Normalizer class doesn't exist");
//		    return false;
//		}
//
//		normalizer = nm;
//	    }
	    argc--;
	}

	return true;
    }

    public int run(String[] arg0) throws Exception
    {
	conf = new Configuration();
	configer = new SimpleConfigurationManager();

	if (!readInput(arg0))
	{
	    printUsage();
	    return -1;
	}

	configer.applyConfig(conf);

	String inputPath = inputBase;

	conf.set("Cloud9.InputPath", inputPath);
	conf.setInt("Cloud9.Mappers", 1);
	conf.setInt("Cloud9.Reducers", 200);
	//conf.set("Cloud9.AnchorTextNormalizer", normalizer);

	// for each element tag, do truncating
	for (String tag : elementTags)
	{
	    String outputPath = outputBase + "/" + "tag_" + tag;

	    conf.set("Cloud9.OutputPath", outputPath);
	    conf.set("Cloud9.targetTag", tag);

	    int r = new DocumentElementTruncater(conf, configer).run();

	    if (r != 0)
	    {
		return -1;
	    }
	}

	return 0;
    }
}
