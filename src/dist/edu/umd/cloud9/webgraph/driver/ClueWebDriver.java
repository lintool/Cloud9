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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.webgraph.BuildReverseWebGraph;
import edu.umd.cloud9.webgraph.BuildWebGraph;
import edu.umd.cloud9.webgraph.CollectHostnames;
import edu.umd.cloud9.webgraph.ComputeWeight;
import edu.umd.cloud9.webgraph.ClueExtractLinks;
import edu.umd.cloud9.webgraph.DriverUtil;

/**
 * <p>
 * Main driver program for extracting the web graph, reverse web graph, and
 * lines of anchor text. Command-line arguments are as follows:
 * </p>
 *
 * <ul>
 * <li>[-input collection-base-path]: the base path to the collection, collection file
 * will be searched recursively</li>
 * <li>[-output output-base-path]: the base path under which the output would be stored</li>
 * <li>[-docno docno-mapping-file]: the path to the docno-mapping.dat file</li>
 * <li>[-begin from-segment]: starting segment number</li>
 * <li>[-end to-segment]: ending segment number</li>
 * <li>[-il]: include internal links (i.e., links within a domain)</li>
 * <li>[-caw]: compute the default weights for lines of external anchor text</li>
 * <li>[-normalizer normalizer] A normalizer class used to normalize the lines of anchor
 * text, must extend edu.umd.cloud9.webgraph.normalize.AnchorTextNormalizer.</li>
 * </ul>
 *
 * <p>
 * The starting and ending segments will correspond to paths
 * <code>/[collection-base-path]/en.XX</code> to
 * <code>/[collection-base-path]/en.YY</code>.
 * </p>
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
 * @author Nima Asadi
 *
 */

public class ClueWebDriver extends Configured implements Tool {
  private static int printUsage() {
    System.out.println("\nusage:" +
                       "-input collection-path" +
                       "-output output-base" +
                       "-docno userSpecifiedDocnoMappingFile " +
                       "-begin begin_segment" +
                       "-end end_segment" +
                       "[-il] " +
                       "[-caw] " +
                       "-normalizer normalizerClass");

    System.out.println("Help:");
    System.out.println("[" + DriverUtil.CL_INPUT + " collection-path]\n\tinput directory");
    System.out.println("[" + DriverUtil.CL_OUTPUT + " output-base]\n\toutput directory");
    System.out.println(DriverUtil.CL_BEGIN_SEGMENT + " begin_segment: First segment to process.");
    System.out.println(DriverUtil.CL_END_SEGMENT + " end_segment: Last segment to process.");
    System.out.println(DriverUtil.CL_DOCNO_MAPPING +
                       " docno mapping file.");
    System.out.println(DriverUtil.CL_INCLUDE_INTERNAL_LINKS +
                       "\n\tinclude internal links, without this" +
                       "option we will not include internal links");
    System.out.println(DriverUtil.CL_COMPUTE_WEIGHTS +
                       "\n\tcompute default anchor weights, without this " +
                       "option we will not compute default anchor weights");
    System.out.println(DriverUtil.CL_NORMALIZER +
                       " normalizerClass\n\ta normalizer class" +
                       " used to normalize the lines of anchor text," +
                       " must extend edu.umd.cloud9.webgraph.normalize.AnchorTextNormalizer.");
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 6) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();
    String inputArg = DriverUtil.argValue(args, DriverUtil.CL_INPUT);
    final String inputBase = inputArg.endsWith("/") ? inputArg : inputArg + "/";
    String outputArg = DriverUtil.argValue(args, DriverUtil.CL_OUTPUT);
    final String outputBase = outputArg.endsWith("/") ? outputArg : outputArg + "/";
    final String docnoMapping = DriverUtil.argValue(args, DriverUtil.CL_DOCNO_MAPPING);
    final int fromSegment = Integer.parseInt(DriverUtil.argValue(args, DriverUtil.CL_BEGIN_SEGMENT));
    final int toSegment = Integer.parseInt(DriverUtil.argValue(args, DriverUtil.CL_END_SEGMENT));
    final boolean includeInternalLinks = DriverUtil.argExists(args, DriverUtil.CL_INCLUDE_INTERNAL_LINKS);
    final boolean computeAnchorWeights = DriverUtil.argExists(args, DriverUtil.CL_COMPUTE_WEIGHTS);
    final String normalizer = DriverUtil.argValue(args, DriverUtil.CL_NORMALIZER);

    conf.setInt("Cloud9.Mappers", 2000);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS);
    conf.set("Cloud9.DocnoMappingFile", docnoMapping);
    conf.setBoolean("Cloud9.IncludeInternalLinks", includeInternalLinks);
    conf.set("Cloud9.AnchorTextNormalizer", normalizer);

    // Extract link information for each segment separately
    for (int i = fromSegment; i <= toSegment; i++) {
      String inputPath = inputBase + "en." + (i == 10 ? "10" : ("0" + i));
      String outputPath = outputBase + DriverUtil.OUTPUT_EXTRACT_LINKS + "/en." +
        (i == 10 ? "10" : ("0" + i));
      conf.set("Cloud9.InputPath", inputPath);
      conf.set("Cloud9.OutputPath", outputPath);
      int r = new ClueExtractLinks(conf).run();
      if (r != 0) {
        return -1;
      }
    }

    // Construct the reverse web graph (i.e., collect incoming link
    // information)
    String inputPath = "";
    for (int i = fromSegment; i < toSegment; i++) {
      inputPath += outputBase + DriverUtil.OUTPUT_EXTRACT_LINKS + "/en.0" + i + "/,";
    }

    if (toSegment == 10) {
      inputPath += outputBase + DriverUtil.OUTPUT_EXTRACT_LINKS + "/en.10/";
    } else {
      inputPath += outputBase + DriverUtil.OUTPUT_EXTRACT_LINKS + "/en.0" + toSegment + "/";
    }

    String outputPath = outputBase + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/";
    conf.set("Cloud9.InputPath", inputPath);
    conf.set("Cloud9.OutputPath", outputPath);
    conf.setInt("Cloud9.Mappers", 1);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS *
                (toSegment - fromSegment + 1));

    int r = new BuildReverseWebGraph(conf).run();
    if (r != 0) {
      return -1;
    }

    // Construct the web graph
    inputPath = outputBase + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/";
    outputPath = outputBase + DriverUtil.OUTPUT_WEBGRAPH + "/";
    conf.set("Cloud9.InputPath", inputPath);
    conf.set("Cloud9.OutputPath", outputPath);
    conf.setInt("Cloud9.Mappers", 1);
    conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS *
                (toSegment - fromSegment + 1));
    r = new BuildWebGraph(conf).run();
    if (r != 0) {
      return -1;
    }

    if (computeAnchorWeights) {
      // Propagating domain names in order to compute anchor weights
      inputPath = outputBase + DriverUtil.OUTPUT_WEBGRAPH + "/";
      outputPath = outputBase + DriverUtil.OUTPUT_HOST_NAMES + "/";
      conf.set("Cloud9.InputPath", inputPath);
      conf.set("Cloud9.OutputPath", outputPath);
      conf.setInt("Cloud9.Mappers", 1);
      conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS *
                  (toSegment - fromSegment + 1));
      r = new CollectHostnames(conf).run();
      if (r != 0) {
        return -1;
      }

      // Compute the weights
      inputPath = outputBase + DriverUtil.OUTPUT_REVERSE_WEBGRAPH + "/," + outputBase + DriverUtil.OUTPUT_HOST_NAMES + "/";
      outputPath = outputBase + DriverUtil.OUTPUT_WEGIHTED_REVERSE_WEBGRAPH + "/";
      conf.set("Cloud9.InputPath", inputPath);
      conf.set("Cloud9.OutputPath", outputPath);
      conf.setInt("Cloud9.Mappers", 1);
      conf.setInt("Cloud9.Reducers", DriverUtil.DEFAULT_REDUCERS *
                  (toSegment - fromSegment + 1));
      r = new ComputeWeight(conf).run();
      if (r != 0) {
        return -1;
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner
      .run(new Configuration(), new ClueWebDriver(), args);
  }
}
