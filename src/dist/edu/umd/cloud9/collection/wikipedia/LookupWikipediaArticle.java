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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool for providing command-line access to page titles given either a docno or
 * a docid. This does not run as a MapReduce job.
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <blockquote>
 *
 * <pre>
 * hadoop jar cloud9all.jar edu.umd.cloud9.collection.wikipedia.LookupWikipediaArticle \
 *   /user/jimmy/Wikipedia/compressed.block/findex-en-20101011.dat \
 *   /user/jimmy/Wikipedia/docno-en-20101011.dat
 * </pre>
 *
 * </blockquote>
 *
 * @author Jimmy Lin
 *
 */
public class LookupWikipediaArticle extends Configured implements Tool {

	private LookupWikipediaArticle() {
	}

	private static int printUsage() {
		System.out.println("usage:  [forward-index-path] [docno-mapping-data-file]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		WikipediaForwardIndex f = new WikipediaForwardIndex(getConf());
		f.loadIndex(args[0], args[1]);

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

			if (tokens[0].equals("docno")) {
				page = f.getDocument(Integer.parseInt(tokens[1]));
				System.out.println(page.getDocid() + ": " + page.getTitle());
			} else if (tokens[0].equals("docid")) {
				page = f.getDocument(tokens[1]);
				System.out.println(page.getDocid() + ": " + page.getTitle());
			}

			System.out.print("lookup > ");
		}

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new LookupWikipediaArticle(), args);
		System.exit(res);
	}
}
