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

package edu.umd.cloud9.collection.wikipedia;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool for providing command-line access to page titles given either a docno or a docid. This does
 * not run as a MapReduce job.
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <blockquote><pre>
 * etc/hadoop-cluster.sh edu.umd.cloud9.collection.wikipedia.LookupWikipediaArticle \
 *  enwiki-20130503.findex.dat enwiki-20130503-docno.dat
 * </pre></blockquote>
 * 
 * @author Jimmy Lin
 */
public class LookupWikipediaArticle extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("usage: [forward-index-path] [docno-mapping-data-file]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Configuration conf = getConf();
    WikipediaForwardIndex f = new WikipediaForwardIndex(conf);
    f.loadIndex(new Path(args[0]), new Path(args[1]), FileSystem.get(conf));

    WikipediaPage page;

    System.out.println(" \"docno [no]\" or \"docid [id]\" to lookup documents");
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("lookup > ");
    while ((cmd = stdin.readLine()) != null) {

      String[] tokens = cmd.split("\\s+");

      if (tokens.length != 2) {
        System.out.println("Error: unrecognized command!");
        System.out.print("lookup > ");

        continue;
      }

      if ("docno".equals(tokens[0])) {
        try {
          page = f.getDocument(Integer.parseInt(tokens[1]));
          if (page != null) {
            System.out.println("docid " + page.getDocid() + ": " + page.getTitle());
          } else {
            System.out.println("docno " + tokens[1] + " not found!");
          }
        } catch (NumberFormatException e) {
          System.out.println("Invalid docno " + tokens[1]);
        }
      } else if ("docid".equals(tokens[0])) {
        page = f.getDocument(tokens[1]);
        if (page != null) {
          System.out.println("docid " + page.getDocid() + ": " + page.getTitle());
        } else {
          System.out.println("docid " + tokens[1] + " not found!");
        }
      }

      System.out.print("lookup > ");
    }

    return 0;
  }

  private LookupWikipediaArticle() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupWikipediaArticle(), args);
  }
}
