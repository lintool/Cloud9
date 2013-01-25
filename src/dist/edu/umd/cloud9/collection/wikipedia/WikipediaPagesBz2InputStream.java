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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.tools.bzip2.CBZip2InputStream;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Class for working with bz2-compressed Wikipedia article dump files on local
 * disk.
 * 
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaPagesBz2InputStream {
	private static int DEFAULT_STRINGBUFFER_CAPACITY = 1024;

	private BufferedReader br;
	private FileInputStream fis;

	/**
	 * Creates an input stream for reading Wikipedia articles from a
	 * bz2-compressed dump file.
	 * 
	 * @param file
	 *            path to dump file
	 * @throws IOException
	 */
	public WikipediaPagesBz2InputStream(String file) throws IOException {
		br = null;
		fis = new FileInputStream(file);
		byte[] ignoreBytes = new byte[2];
		fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
		br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis)));

	}

	/**
	 * Reads the next Wikipedia page.
	 * 
	 * @param page
	 *            WikipediaPage object to read into
	 * @return <code>true</code> if page is successfully read
	 * @throws IOException
	 */
	public boolean readNext(WikipediaPage page) throws IOException {
		String s = null;
		StringBuffer sb = new StringBuffer(DEFAULT_STRINGBUFFER_CAPACITY);

		while ((s = br.readLine()) != null) {
			if (s.endsWith("<page>"))
				break;
		}

		if (s == null) {
			fis.close();
			br.close();
			return false;
		}

		sb.append(s + "\n");

		while ((s = br.readLine()) != null) {
			sb.append(s + "\n");

			if (s.endsWith("</page>"))
				break;
		}

		WikipediaPage.readPage(page, sb.toString());

		return true;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("usage: [file] [language]");
			System.exit(-1);
		}

		WikipediaPage p = WikipediaPageFactory.createWikipediaPage(args[1]);
		
		WikipediaPagesBz2InputStream stream = new WikipediaPagesBz2InputStream(args[0]);
		while (stream.readNext(p)) {
			System.out.println(p.getContent());
		}
	}
}
