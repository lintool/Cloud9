package edu.umd.cloud9.data.wikipedia;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.data.IdDocnoMapping;

public class WikipediaDocnoMapping implements IdDocnoMapping {
	String[] mTitles;

	public int getDocno(String docid) {
		return Arrays.binarySearch(mTitles, docid);
	}

	public String getDocid(int docno) {
		return mTitles[docno];
	}

	public void loadMapping(Path p, FileSystem fs) throws IOException {
		mTitles = NumberWikipediaArticles.readArticleTitlesData(p, fs);
	}
}
