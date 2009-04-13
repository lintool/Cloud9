package edu.umd.cloud9.collection.wikipedia;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.tools.bzip2.CBZip2InputStream;

public class WikipediaPagesBz2InputStream {
	private static int DEFAULT_STRINGBUFFER_CAPACITY = 1024;

	BufferedReader br;
	FileInputStream fis;

	public WikipediaPagesBz2InputStream(String file) throws IOException {
		br = null;
		fis = new FileInputStream(file);
		byte[] ignoreBytes = new byte[2];
		fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
		br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis)));

	}

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

}
