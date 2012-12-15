package edu.umd.cloud9.integration;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class IntegrationUtils {
  public static final String D_JT = "-Dmapred.job.tracker=bespin00.umiacs.umd.edu:8021";
  public static final String D_NN = "-Dfs.defaultFS=hdfs://bespinrm.umiacs.umd.edu:8020";

  public static final String D_JT_LOCAL = "-D mapred.job.tracker=local";
  public static final String D_NN_LOCAL = "-D fs.default.name=file:///";

  public static String getJar(String path, final String prefix) {
      File[] arr = new File(path).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(prefix) && !name.contains("javadoc") && !name.contains("sources");
      }
    });

    assertTrue(arr.length == 1);
    return arr[0].getAbsolutePath();
  }

  public static Configuration getBespinConfiguration() {
    Configuration conf = new Configuration();

    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

    conf.reloadConfiguration();

    return conf;
  }
}
