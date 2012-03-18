package edu.umd.cloud9.webgraph.driver.wt10g;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcInputFormat2;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat2;
import edu.umd.cloud9.collection.trecweb.Gov2DocnoMapping;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat2;
import edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping;

public class CollectionConfigurationManager
{
	public static final String[] supported = { "clue", "trecweb", "gov2", "wt10g" };// if one
																	// collection's
																	// name
																	// contain
																	// name of
	// another in beginning, it should appear first.
	// public FileInputFormat fileFormatAgent = null;

	private boolean userSpecifiedInputFormat = false;
	private boolean userSpecifiedDocnoMapping = false;
	private int tgtConf = -1;
	private Class userSpecifiedInputFormatClass;
	private String userSpecifiedDocnoMappingClass;

	public static boolean isSupport(String tgtCollection)
	{
		return (getSupportIndex(tgtCollection) != -1);
	}

	private static int getSupportIndex(String tgtCollection)
	{
		tgtCollection = tgtCollection.toLowerCase();
		for (int i = 0; i < supported.length; i++)
			if (tgtCollection.startsWith(supported[i]))
				return i;
		return -1;
	}

	public boolean setConfByCollection(String collectionName)
	{
		int index = getSupportIndex(collectionName);
		if (index == -1)
		{
			return false;
		}
		tgtConf = index;
		return true;
	}

	public boolean setUserSpecifiedInputFormat(String className)
	{
		// It must be a class file
//		if (!className.endsWith(".class"))
//			return false;

		Class userClass;
		try
		{
			userClass = Class.forName(className);
		}
		catch (ClassNotFoundException e)
		{
			return false;
		}

		// It has to be sub class of FileInputFormat
		if (!FileInputFormat.class.isAssignableFrom(userClass))
			return false;

		userSpecifiedInputFormat = true;
		userSpecifiedInputFormatClass = userClass;

		return true;
	}

	public boolean setUserSpecifiedDocnoMappingClass(String className)
	{
		Class userClass;
		try
		{
			userClass = Class.forName(className);
		}
		catch (ClassNotFoundException e)
		{
			return false;
		}

		// It has to be sub class of DocnoMapping
		if (!DocnoMapping.class.isAssignableFrom(userClass))
			return false;

		userSpecifiedDocnoMapping = true;
		userSpecifiedDocnoMappingClass = className;

		return true;
	}

//	public DocnoMapping generateDocmapping(Configuration conf) throws Exception
//	{
//		if (userSpecifiedDocnoMapping)
//		{
//			GenericDocnoMappingInterface dm = (GenericDocnoMappingInterface) userSpecifiedDocnoMappingClass
//					.newInstance();
//			for (String required : dm.requiredConf())
//			{
//				if (conf.get(required, "").equals(""))
//				{
//					System.out.println("Property \"" + required
//							+ "\" is required in configuration.");
//					throw new Exception("Property required");
//				}
//			}
//			dm.preConfig(conf);
//			return dm;
//		}
//		else
//		{
//			switch (tgtConf)
//			{
//				case 0:
//					ClueWarcDocnoMapping doc = new ClueWarcDocnoMapping();
//
//					// TODO
//					// loading doc, not done yet..
//
//					return doc;
//				case 1:
//					return new TrecDocnoMapping();
//				case 3:
//					return new Wt10gDocnoMapping();
//				case 4:
//					return new Gov2DocnoMapping();
//
//					// TODO, the input file stuff is not solved well.
//			}
//		}
//		return null;
//	}

	public void applyJobConfig(Job job) throws Exception
	{
		if (userSpecifiedInputFormat)
		{
			job.setInputFormatClass(userSpecifiedInputFormatClass);
		}
		else
		{
			switch (tgtConf)
			{
				case 0:
					job.setInputFormatClass(ClueWarcInputFormat2.class);
					break;
				case 1: case 2: case 3:
					job.setInputFormatClass(TrecWebDocumentInputFormat2.class);
					break;
				default:
					throw new Exception("InputFormat class not specified");
			}
		}
	}
	
	public void applyConfig(Configuration conf) throws Exception
	{
		if (userSpecifiedDocnoMapping)
		{
			conf.set("Cloud9.DocnoMappingClass", userSpecifiedDocnoMappingClass);
		}
		else
		{
			switch (tgtConf)
			{
				case 0:
					conf.set("Cloud9.DocnoMappingClass", "edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping");
					break;
				case 2: 
					conf.set("Cloud9.DocnoMappingClass", "edu.umd.cloud9.collection.trecweb.Gov2DocnoMapping");
					break;
				case 3:
					conf.set("Cloud9.DocnoMappingClass", "edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping");
					break;
				case 1: default:
					throw new Exception("DocnoMapping class not specified");
			}
		}
	}
	
}
