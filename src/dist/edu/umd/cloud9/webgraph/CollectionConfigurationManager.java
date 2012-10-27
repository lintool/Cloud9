package edu.umd.cloud9.webgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat;

public class CollectionConfigurationManager {
  public static final String[] supported = { "trecweb", "gov2", "wt10g" };

  private boolean userSpecifiedInputFormat = false;
  private boolean userSpecifiedDocnoMapping = false;
  private int tgtConf = -1;
  private Class<? extends InputFormat<?, ?>> userSpecifiedInputFormatClass;
  private String userSpecifiedDocnoMappingClass;

  public static boolean isSupported(String tgtCollection) {
    return (getCollectionIndex(tgtCollection) >= 0);
  }

  private static int getCollectionIndex(String tgtCollection) {
    tgtCollection = tgtCollection.toLowerCase();
    for (int i = 0; i < supported.length; i++)
      if (tgtCollection.startsWith(supported[i]))
        return i;
    return -1;
  }

  public boolean setConfByCollection(String collectionName) {
    int index = getCollectionIndex(collectionName);
    if (index == -1) {
      return false;
    }
    tgtConf = index;
    return true;
  }

  @SuppressWarnings("unchecked")
  public boolean setUserSpecifiedInputFormat(String className) {
    Class<? extends InputFormat<?, ?>> userClass;
    try {
      userClass = (Class<? extends InputFormat<?, ?>>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      return false;
    }

    // It has to be sub class of FileInputFormat
    if (!FileInputFormat.class.isAssignableFrom(userClass)) {
      return false;
    }

    userSpecifiedInputFormat = true;
    userSpecifiedInputFormatClass = userClass;

    return true;
  }

  @SuppressWarnings("unchecked")
  public boolean setUserSpecifiedDocnoMappingClass(String className) {
    Class<? extends DocnoMapping> userClass;
    try {
      userClass = (Class<? extends DocnoMapping>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      return false;
    }

    // It has to be sub class of DocnoMapping
    if (!DocnoMapping.class.isAssignableFrom(userClass)) {
      return false;
    }

    userSpecifiedDocnoMapping = true;
    userSpecifiedDocnoMappingClass = className;

    return true;
  }

  public void applyJobConfig(Job job) throws Exception {
    if (userSpecifiedInputFormat) {
      job.setInputFormatClass(userSpecifiedInputFormatClass);
    } else {
      switch (tgtConf) {
      case 0:
      case 1:
      case 2:
        job.setInputFormatClass(TrecWebDocumentInputFormat.class);
        break;
      default:
        throw new Exception("InputFormat class not specified");
      }
    }
  }

  public void applyConfig(Configuration conf) throws Exception {
    if (userSpecifiedDocnoMapping) {
      conf.set("Cloud9.DocnoMappingClass", userSpecifiedDocnoMappingClass);
    } else {
      switch (tgtConf) {
      case 1:
        conf.set("Cloud9.DocnoMappingClass",
            edu.umd.cloud9.collection.trecweb.Gov2DocnoMapping.class.getCanonicalName());
        break;
      case 2:
        conf.set("Cloud9.DocnoMappingClass",
            edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping.class.getCanonicalName());
        break;
      case 0:
      default:
        throw new Exception("DocnoMapping class not specified");
      }
    }
  }
}
