/*
 * Cloud9: A MapReduce Library for Hadoop
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

package edu.umd.cloud9.webgraph.driver;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.webgraph.BuildReverseWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.CollectionConfigurationManager;
import edu.umd.cloud9.webgraph.CollectHostnames;
import edu.umd.cloud9.webgraph.ComputeWeight;
import edu.umd.cloud9.webgraph.DriverUtil;
import edu.umd.cloud9.webgraph.TrecExtractLinks;
import edu.umd.cloud9.webgraph.normalizer.AnchorTextNormalizer;

/**
 * <p>
 * Main driver program for extracting the web graph, reverse web graph, and
 * lines of anchor text. Command-line arguments are as follows:
 * </p>
 *
 * <ul>
 * <li>[-input collection_base_path] the base path to the collection</li>
 * <li>[-output output-base-path]: the base path under which the output would be stored</li>
 * <li>[-collection {trecweb|trec|wt10g|gov2}]: the collection used</li>
 * <li>[-inputFormat inputFormatClass]: use user specified input format
 * class</li>
 * <li>[-docnoClass userSpecifiedDocnoMappingClass]: use user specified docno mapping
 * class</li>
 * <li>[-il]: use this for including the internal links (i.e., links within a
 * domain) remove for not</li>
 * <li>[-caw]: use this to compute the default weights for lines of external
 * anchor text, remove for not</li>
 * <li>[-normalizer normalizer] A normalizer class used to normalize the lines of anchor
 * text, must extend *.anchor.normalize.AnchorTextNormalizer.</li>
 * <li>[<key:value> ..]: key-value pairs to put in configuration files. It shall
 * also be used as input method for user specified classes</li>
 * </ul>
 *
 * <p>
 * The default weight used in this program was originally proposed by Metzler
 * et. al in the following paper: <br />
 *
 * D. Metzler, J. Novak, H. Cui, and S. Reddy. Building enriched document
 * representations using aggregated anchor text. <i>In Proc. 32nd Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval</i>, pages 219{226, New York, NY, USA, 2009. ACM.
 * </p>
 *
 * @author Nima Asadi , Modified by Fangyue Wang
 *
 */

public class TrecDriver extends Configured implements Tool {
  private String inputBase;
  private String outputBase;
  private boolean includeInternalLinks = false;
  private boolean computeAnchorWeights = false;
  private String normalizer = "edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer";
  private String filtername = null;
  private Configuration conf;
  private CollectionConfigurationManager configer;

  public int run(String[] args) throws Exception {
    conf = getConf();
    configer = new CollectionConfigurationManager();
    if (!readInput(args)) {
      printUsage();
      return -1;
    }

    configer.applyConfig(conf);
    conf.setInt("Cloud9.Mappers", 2000);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
    conf.setBoolean("Cloud9.IncludeInternalLinks", includeInternalLinks);
    conf.set("Cloud9.AnchorTextNormalizer", normalizer);

    // Job 1:
    // Extract link information for each segment separately
    String inputPath = inputBase;
    String outputPath = outputBase + "/" + DriverUtil.OUTPUT_EXTRACT_LINKS;

    conf.set("Cloud9.InputPath", inputPath);
    conf.set("Cloud9.OutputPath", outputPath);
    int r = new TrecExtractLinks(conf, configer).run();
    if (r != 0) {
      return -1;
    }

    // Job 2:
    // Construct the reverse web graph (i.e., collect incoming link
    // information)
    inputPath = outputBase + "/" + DriverUtil.OUTPUT_EXTRACT_LINKS;
    outputPath = outputBase + "/" + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/";
    conf.set("Cloud9.InputPath", inputPath);
    conf.set("Cloud9.OutputPath", outputPath);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
    r = new BuildReverseWebGraph(conf).run();
    if (r != 0) {
      return -1;
    }

    // Job 3:
    // Construct the web graph
    inputPath = outputBase + "/" + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/";
    outputPath = outputBase + "/" + DriverUtil.OUTPUT_WEBGRAPH + "/";
    conf.set("Cloud9.InputPath", inputPath);
    conf.set("Cloud9.OutputPath", outputPath);
    conf.setInt("Cloud9.Mappers", 1);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
    r = new BuildWebGraph(conf).run();
    if (r != 0) {
      return -1;
    }

    if (computeAnchorWeights) {
      // Propagating domain names in order to compute anchor weights
      inputPath = outputBase + "/" + DriverUtil.OUTPUT_WEBGRAPH + "/";
      outputPath = outputBase + "/" + DriverUtil.OUTPUT_HOST_NAMES + "/";
      conf.set("Cloud9.InputPath", inputPath);
      conf.set("Cloud9.OutputPath", outputPath);
      conf.setInt("Cloud9.Mappers", 1);
      conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
      r = new CollectHostnames(conf).run();
      if (r != 0) {
        return -1;
      }

      // Compute the weights
      inputPath = outputBase + "/" + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/," +
        outputBase + "/" + DriverUtil.OUTPUT_HOST_NAMES + "/";
      outputPath = outputBase + "/" + DriverUtil.OUTPUT_WEGIHTED_REVERSE_WEBGRAPH + "/";
      conf.set("Cloud9.InputPath", inputPath);
      conf.set("Cloud9.OutputPath", outputPath);
      conf.setInt("Cloud9.Mappers", 1);
      conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
      r = new ComputeWeight(conf).run();
      if (r != 0) {
        return -1;
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner
      .run(new Configuration(), new TrecDriver(), args);
  }

  private static int printUsage() {
    System.out.println("\nusage:" +
                       "[-input collection-path]" +
                       "[-output output-base" +
                       "[-collection {trecweb|gov2|wt10g}] " +
                       "[-inputFormat userSpecifiedInputFormatClass] " +
                       "[-docnoClass userSpecifiedDocnoMappingClass] " +
                       "-docno userSpecifiedDocnoMappingFile " +
                       "[-il] " +
                       "[-caw] " +
                       "[-normalizer normalizerClass] ");
    System.out.println("Help:");
    System.out.println("[" + DriverUtil.CL_INPUT + " collection-path]\n\tinput directory");
    System.out.println("[" + DriverUtil.CL_OUTPUT + " output-base]\n\toutput directory");
    System.out
      .println(DriverUtil.CL_COLLECTION + " {trecweb|gov2|wt10g}\n\tname the collection name, if it is supported, automatic configuration will be applied");
    System.out
      .println(DriverUtil.CL_INPUT_FORMAT + " userSpecifiedInputFormatClass\n\tspecify the class work as FileInputFormat;" +
               " Required when -collection is not specified");
    System.out
      .println(DriverUtil.CL_DOCNO_MAPPING_CLASS + " userSpecifiedDocnoMappingClass\n\tspecify the class work as DocnoMapping;" +
               "Required when -collection is not specified. It should implement GenericDocnoMapping interface.");
    System.out
      .println(DriverUtil.CL_DOCNO_MAPPING + " userSpecifiedDocnoMappingFile\n\tspecify the File work as input to specified DocnoMapping class.");
    System.out
      .println(DriverUtil.CL_INCLUDE_INTERNAL_LINKS + "\n\tinclude internal links, without this option we will not include internal links");
    System.out
      .println(DriverUtil.CL_COMPUTE_WEIGHTS + "\n\tcompute default anchor weights, without this option we will not compute default anchor weights");
    System.out
      .println(DriverUtil.CL_NORMALIZER + " normalizerClass\n\ta normalizer class used to normalize the lines of anchor text," +
               " must extend edu.umd.cloud9.webgraph.normalize.AnchorTextNormalizer.");
    System.out.println();

    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  private boolean readInput(String[] args) {
    if (args.length < 6) {
      System.out.println("More arguments needed.");
      return false;
    }

    inputBase = new File(DriverUtil.argValue(args, DriverUtil.CL_INPUT)).getAbsolutePath();
    outputBase = new File(DriverUtil.argValue(args, DriverUtil.CL_OUTPUT)).getAbsolutePath();

    boolean knownCollection = DriverUtil.argExists(args, DriverUtil.CL_COLLECTION);

    if(knownCollection) {
      String collectionName = DriverUtil.argValue(args, DriverUtil.CL_COLLECTION);
      if (!configer.setConfByCollection(collectionName)) {
        System.out.println("Collection \"" + collectionName +
                           "\" not supported, please specify inputformat and docnomapping class, or contact developer.");
        return false;
      }
    } else {
      String ciName = DriverUtil.argValue(args, DriverUtil.CL_INPUT_FORMAT);
      if (!configer.setUserSpecifiedInputFormat(ciName)) {
        System.out.println("class \"" + ciName +
                           "\" doesn't exist or not sub-class of FileInputFormat");
        return false;
      }
      String cmName = DriverUtil.argValue(args, DriverUtil.CL_DOCNO_MAPPING_CLASS);
      if (!configer.setUserSpecifiedDocnoMappingClass(cmName)) {
        System.out.println("class \"" + cmName +
                           "\" doesn't exist or not implemented DocnoMappingt");
        return false;
      }
    }

    conf.set("Cloud9.DocnoMappingFile", DriverUtil.argValue(args, DriverUtil.CL_DOCNO_MAPPING));
    includeInternalLinks = DriverUtil.argExists(args, DriverUtil.CL_INCLUDE_INTERNAL_LINKS);
    computeAnchorWeights = DriverUtil.argExists(args, DriverUtil.CL_COMPUTE_WEIGHTS);

    String nm = DriverUtil.argValue(args, DriverUtil.CL_NORMALIZER);
    try {
      if (!AnchorTextNormalizer.class.isAssignableFrom(Class.forName(nm))) {
        System.out
          .println("Invalid arguments; Normalizer class must implement AnchorTextNormalizer interface.");
        return false;
      }
    } catch (ClassNotFoundException e) {
      System.out
        .println("Invalid arguments; Specified Normalizer class doesn't exist");
      return false;
    }

    normalizer = nm;
    return true;
  }
}
