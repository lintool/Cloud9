package edu.umd.cloud9.integration;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class IntegrationUtils {
  public static final String LOCAL_ARGS = 
      "-D fs.defaultFS=file:/// " +
      "-D mapreduce.framework.name=local " +
      "-D mapreduce.jobtracker.address=local " +
      "-D mapreduce.cluster.local.dir=/tmp/mapred/local " +
      "-D mapreduce.cluster.temp.dir=/tmp/mapred/temp " +
      "-D mapreduce.jobtracker.staging.root.dir=/tmp/mapred/staging " +
      "-D mapreduce.jobtracker.system.dir=/tmp/mapred/system";

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

  // How to properly shell out: http://www.javaworld.com/javaworld/jw-12-2000/jw-1229-traps.html
  public static int exec(String cmd) throws IOException, InterruptedException {
    System.out.println("Executing command: " + cmd);

    Runtime rt = Runtime.getRuntime();
    Process proc = rt.exec(cmd);

    // any error message?
    StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "STDERR");

    // any output?
    StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "STDOUT");

    // kick them off
    errorGobbler.start();
    outputGobbler.start();

    // any error???
    int exitVal = proc.waitFor();
    System.out.println("ExitValue: " + exitVal);
    return exitVal;
  }

  public static List<Integer> execWiki(String cmd) throws IOException, InterruptedException {
    System.out.println("Executing command: " + cmd);

    Runtime rt = Runtime.getRuntime();
    Process proc = rt.exec(cmd);

    // any error message?
    WikiGobbler errorGobbler = new WikiGobbler(proc.getErrorStream(), "STDERR");

    // any output?
    StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "STDOUT");

    // kick them off
    errorGobbler.start();
    outputGobbler.start();

    // any error???
    int exitVal = proc.waitFor();
    System.out.println("ExitValue: " + exitVal);

    return ImmutableList.of(errorGobbler.disambCount, errorGobbler.articleCount, errorGobbler.totalCount);
  }

  public static Map<String, Integer> execKeyValueExtractor(String cmd, Set<String> keys)
      throws IOException, InterruptedException {
    System.out.println("Executing command: " + cmd);

    Runtime rt = Runtime.getRuntime();
    Process proc = rt.exec(cmd);

    // any error message?
    KeyValuePairsGobbler errorGobbler = new KeyValuePairsGobbler(proc.getErrorStream(), "STDERR", keys);

    // any output?
    StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "STDOUT");

    // kick them off
    errorGobbler.start();
    outputGobbler.start();

    // any error???
    int exitVal = proc.waitFor();
    System.out.println("ExitValue: " + exitVal);

    return errorGobbler.map;
  }

  private static class StreamGobbler extends Thread {
    InputStream is;
    String type;

    StreamGobbler(InputStream is, String type) {
      this.is = is;
      this.type = type;
    }

    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null)
          System.out.println(type + ">" + line);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  private static class WikiGobbler extends StreamGobbler {
    int disambCount = 0;
    int articleCount = 0;
    int totalCount = 0;
    
    WikiGobbler(InputStream is, String type) {
      super(is, type);
    }

    // depends on PageType names and handling in BuildWikipediaDocnoMapping
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          System.out.println(type + ">" + line);
          if (line.contains("DISAMBIGUATION=")) {
            String[] arr = line.trim().split("DISAMBIGUATION=");
            disambCount = Integer.parseInt(arr[1]);
          } else if (line.contains("ARTICLE=") && !line.contains("NON_ARTICLE=")) {
            String[] arr = line.trim().split("ARTICLE=");
            articleCount = Integer.parseInt(arr[1]);
          } else if (line.contains("TOTAL=")) {
            String[] arr = line.trim().split("TOTAL=");
            totalCount = Integer.parseInt(arr[1]);
          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  private static class KeyValuePairsGobbler extends StreamGobbler {
    Map<String, Integer> map = Maps.newHashMap();
    private Set<String> keys;
    
    KeyValuePairsGobbler(InputStream is, String type, Set<String> keys) {
      super(is, type);
      this.keys = keys;
    }

    // depends on PageType names and handling in BuildWikipediaDocnoMapping
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          System.out.println(type + ">" + line);

          for ( String key : keys) {
            if (line.contains(key + "=")) {
              String[] arr = line.trim().split(key + "=");
              map.put(key, Integer.parseInt(arr[1]));
            }            
          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

}
