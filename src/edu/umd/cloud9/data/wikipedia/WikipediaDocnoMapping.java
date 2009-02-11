package edu.umd.cloud9.data.wikipedia;

import java.io.IOException;
import java.util.Arrays;

import edu.umd.cloud9.data.IdDocnoMapping;

public class WikipediaDocnoMapping implements IdDocnoMapping {
	String[] mTitles;

	public int getDocno(String docid) {
		return Arrays.binarySearch(mTitles, docid);
	}

	public void loadMapping(String f) throws IOException {
		mTitles = NumberWikipediaArticles.readArticleTitlesData(f);
	}
}
