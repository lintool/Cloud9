package edu.umd.cloud9.collection.clue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Preconditions;

/**
 * Class that provides convenience methods for processing portions of the Clue
 * Web collection with Hadoop. Static methods in this class allow the user to
 * easily "select" different portions of the collection to serve as input to a
 * MapReduce job.
 *
 * @author Jimmy Lin
 */
public class ClueCollectionPathConstants {

	private static final String[] sEnglishTest = { "ClueWeb09_English_1/en0000/00.warc.gz" };

	private static final String[] sEnglishTiny = { "ClueWeb09_English_1/en0000/" };

	private static final String[] sEnglishSmall = { "ClueWeb09_English_1/en0000/",
			"ClueWeb09_English_1/en0001/", "ClueWeb09_English_1/en0002/",
			"ClueWeb09_English_1/en0003/", "ClueWeb09_English_1/en0004/",
			"ClueWeb09_English_1/en0005/", "ClueWeb09_English_1/en0006/",
			"ClueWeb09_English_1/en0007/", "ClueWeb09_English_1/en0008/",
			"ClueWeb09_English_1/en0009/", "ClueWeb09_English_1/en0010/",
			"ClueWeb09_English_1/en0011/", "ClueWeb09_English_1/enwp00/",
			"ClueWeb09_English_1/enwp01/", "ClueWeb09_English_1/enwp02/",
			"ClueWeb09_English_1/enwp03/" };

	private static final String[][] sEnglishSections = {
			{ "ClueWeb09_English_1/en0000/", "ClueWeb09_English_1/en0001/",
					"ClueWeb09_English_1/en0002/", "ClueWeb09_English_1/en0003/",
					"ClueWeb09_English_1/en0004/", "ClueWeb09_English_1/en0005/",
					"ClueWeb09_English_1/en0006/", "ClueWeb09_English_1/en0007/",
					"ClueWeb09_English_1/en0008/", "ClueWeb09_English_1/en0009/",
					"ClueWeb09_English_1/en0010/", "ClueWeb09_English_1/en0011/",
					"ClueWeb09_English_1/enwp00/", "ClueWeb09_English_1/enwp01/",
					"ClueWeb09_English_1/enwp02/", "ClueWeb09_English_1/enwp03/" },
			{ "ClueWeb09_English_2/en0012", "ClueWeb09_English_2/en0013",
					"ClueWeb09_English_2/en0014", "ClueWeb09_English_2/en0015",
					"ClueWeb09_English_2/en0016", "ClueWeb09_English_2/en0017",
					"ClueWeb09_English_2/en0018", "ClueWeb09_English_2/en0019",
					"ClueWeb09_English_2/en0020", "ClueWeb09_English_2/en0021",
					"ClueWeb09_English_2/en0022", "ClueWeb09_English_2/en0023",
					"ClueWeb09_English_2/en0024", "ClueWeb09_English_2/en0025",
					"ClueWeb09_English_2/en0026" },
			{ "ClueWeb09_English_3/en0027", "ClueWeb09_English_3/en0028",
					"ClueWeb09_English_3/en0029", "ClueWeb09_English_3/en0030",
					"ClueWeb09_English_3/en0031", "ClueWeb09_English_3/en0032",
					"ClueWeb09_English_3/en0033", "ClueWeb09_English_3/en0034",
					"ClueWeb09_English_3/en0035", "ClueWeb09_English_3/en0036",
					"ClueWeb09_English_3/en0037", "ClueWeb09_English_3/en0038",
					"ClueWeb09_English_3/en0039", "ClueWeb09_English_3/en0040" },
			{ "ClueWeb09_English_4/en0041", "ClueWeb09_English_4/en0042",
					"ClueWeb09_English_4/en0043", "ClueWeb09_English_4/en0044",
					"ClueWeb09_English_4/en0045", "ClueWeb09_English_4/en0046",
					"ClueWeb09_English_4/en0047", "ClueWeb09_English_4/en0048",
					"ClueWeb09_English_4/en0049", "ClueWeb09_English_4/en0050",
					"ClueWeb09_English_4/en0051", "ClueWeb09_English_4/en0052",
					"ClueWeb09_English_4/en0053", "ClueWeb09_English_4/en0054" },
			{ "ClueWeb09_English_5/en0055", "ClueWeb09_English_5/en0056",
					"ClueWeb09_English_5/en0057", "ClueWeb09_English_5/en0058",
					"ClueWeb09_English_5/en0059", "ClueWeb09_English_5/en0060",
					"ClueWeb09_English_5/en0061", "ClueWeb09_English_5/en0062",
					"ClueWeb09_English_5/en0063", "ClueWeb09_English_5/en0064",
					"ClueWeb09_English_5/en0065", "ClueWeb09_English_5/en0066",
					"ClueWeb09_English_5/en0067", "ClueWeb09_English_5/en0068" },
			{ "ClueWeb09_English_6/en0069", "ClueWeb09_English_6/en0070",
					"ClueWeb09_English_6/en0071", "ClueWeb09_English_6/en0072",
					"ClueWeb09_English_6/en0073", "ClueWeb09_English_6/en0074",
					"ClueWeb09_English_6/en0075", "ClueWeb09_English_6/en0076",
					"ClueWeb09_English_6/en0077", "ClueWeb09_English_6/en0078",
					"ClueWeb09_English_6/en0079", "ClueWeb09_English_6/en0080",
					"ClueWeb09_English_6/en0081", "ClueWeb09_English_6/en0082" },
			{ "ClueWeb09_English_7/en0083", "ClueWeb09_English_7/en0084",
					"ClueWeb09_English_7/en0085", "ClueWeb09_English_7/en0086",
					"ClueWeb09_English_7/en0087", "ClueWeb09_English_7/en0088",
					"ClueWeb09_English_7/en0089", "ClueWeb09_English_7/en0090",
					"ClueWeb09_English_7/en0091", "ClueWeb09_English_7/en0092",
					"ClueWeb09_English_7/en0093", "ClueWeb09_English_7/en0094",
					"ClueWeb09_English_7/en0095", "ClueWeb09_English_7/en0096" },
			{ "ClueWeb09_English_8/en0097", "ClueWeb09_English_8/en0098",
					"ClueWeb09_English_8/en0099", "ClueWeb09_English_8/en0100",
					"ClueWeb09_English_8/en0101", "ClueWeb09_English_8/en0102",
					"ClueWeb09_English_8/en0103", "ClueWeb09_English_8/en0104",
					"ClueWeb09_English_8/en0105", "ClueWeb09_English_8/en0106",
					"ClueWeb09_English_8/en0107", "ClueWeb09_English_8/en0108",
					"ClueWeb09_English_8/en0109" },
			{ "ClueWeb09_English_9/en0110", "ClueWeb09_English_9/en0111",
					"ClueWeb09_English_9/en0112", "ClueWeb09_English_9/en0113",
					"ClueWeb09_English_9/en0114", "ClueWeb09_English_9/en0115",
					"ClueWeb09_English_9/en0116", "ClueWeb09_English_9/en0117",
					"ClueWeb09_English_9/en0118", "ClueWeb09_English_9/en0119",
					"ClueWeb09_English_9/en0120", "ClueWeb09_English_9/en0121",
					"ClueWeb09_English_9/en0122", "ClueWeb09_English_9/en0123" },
			{ "ClueWeb09_English_10/en0124", "ClueWeb09_English_10/en0125",
					"ClueWeb09_English_10/en0126", "ClueWeb09_English_10/en0127",
					"ClueWeb09_English_10/en0128", "ClueWeb09_English_10/en0129",
					"ClueWeb09_English_10/en0130", "ClueWeb09_English_10/en0131",
					"ClueWeb09_English_10/en0132", "ClueWeb09_English_10/en0133" } };

	private static final String[] sEnglishComplete = { "ClueWeb09_English_1/en0000/",
			"ClueWeb09_English_1/en0001/", "ClueWeb09_English_1/en0002/",
			"ClueWeb09_English_1/en0003/", "ClueWeb09_English_1/en0004/",
			"ClueWeb09_English_1/en0005/", "ClueWeb09_English_1/en0006/",
			"ClueWeb09_English_1/en0007/", "ClueWeb09_English_1/en0008/",
			"ClueWeb09_English_1/en0009/", "ClueWeb09_English_1/en0010/",
			"ClueWeb09_English_1/en0011/", "ClueWeb09_English_1/enwp00/",
			"ClueWeb09_English_1/enwp01/", "ClueWeb09_English_1/enwp02/",
			"ClueWeb09_English_1/enwp03/", "ClueWeb09_English_2/en0012",
			"ClueWeb09_English_2/en0013", "ClueWeb09_English_2/en0014",
			"ClueWeb09_English_2/en0015", "ClueWeb09_English_2/en0016",
			"ClueWeb09_English_2/en0017", "ClueWeb09_English_2/en0018",
			"ClueWeb09_English_2/en0019", "ClueWeb09_English_2/en0020",
			"ClueWeb09_English_2/en0021", "ClueWeb09_English_2/en0022",
			"ClueWeb09_English_2/en0023", "ClueWeb09_English_2/en0024",
			"ClueWeb09_English_2/en0025", "ClueWeb09_English_2/en0026",
			"ClueWeb09_English_3/en0027", "ClueWeb09_English_3/en0028",
			"ClueWeb09_English_3/en0029", "ClueWeb09_English_3/en0030",
			"ClueWeb09_English_3/en0031", "ClueWeb09_English_3/en0032",
			"ClueWeb09_English_3/en0033", "ClueWeb09_English_3/en0034",
			"ClueWeb09_English_3/en0035", "ClueWeb09_English_3/en0036",
			"ClueWeb09_English_3/en0037", "ClueWeb09_English_3/en0038",
			"ClueWeb09_English_3/en0039", "ClueWeb09_English_3/en0040",
			"ClueWeb09_English_4/en0041", "ClueWeb09_English_4/en0042",
			"ClueWeb09_English_4/en0043", "ClueWeb09_English_4/en0044",
			"ClueWeb09_English_4/en0045", "ClueWeb09_English_4/en0046",
			"ClueWeb09_English_4/en0047", "ClueWeb09_English_4/en0048",
			"ClueWeb09_English_4/en0049", "ClueWeb09_English_4/en0050",
			"ClueWeb09_English_4/en0051", "ClueWeb09_English_4/en0052",
			"ClueWeb09_English_4/en0053", "ClueWeb09_English_4/en0054",
			"ClueWeb09_English_5/en0055", "ClueWeb09_English_5/en0056",
			"ClueWeb09_English_5/en0057", "ClueWeb09_English_5/en0058",
			"ClueWeb09_English_5/en0059", "ClueWeb09_English_5/en0060",
			"ClueWeb09_English_5/en0061", "ClueWeb09_English_5/en0062",
			"ClueWeb09_English_5/en0063", "ClueWeb09_English_5/en0064",
			"ClueWeb09_English_5/en0065", "ClueWeb09_English_5/en0066",
			"ClueWeb09_English_5/en0067", "ClueWeb09_English_5/en0068",
			"ClueWeb09_English_6/en0069", "ClueWeb09_English_6/en0070",
			"ClueWeb09_English_6/en0071", "ClueWeb09_English_6/en0072",
			"ClueWeb09_English_6/en0073", "ClueWeb09_English_6/en0074",
			"ClueWeb09_English_6/en0075", "ClueWeb09_English_6/en0076",
			"ClueWeb09_English_6/en0077", "ClueWeb09_English_6/en0078",
			"ClueWeb09_English_6/en0079", "ClueWeb09_English_6/en0080",
			"ClueWeb09_English_6/en0081", "ClueWeb09_English_6/en0082",
			"ClueWeb09_English_7/en0083", "ClueWeb09_English_7/en0084",
			"ClueWeb09_English_7/en0085", "ClueWeb09_English_7/en0086",
			"ClueWeb09_English_7/en0087", "ClueWeb09_English_7/en0088",
			"ClueWeb09_English_7/en0089", "ClueWeb09_English_7/en0090",
			"ClueWeb09_English_7/en0091", "ClueWeb09_English_7/en0092",
			"ClueWeb09_English_7/en0093", "ClueWeb09_English_7/en0094",
			"ClueWeb09_English_7/en0095", "ClueWeb09_English_7/en0096",
			"ClueWeb09_English_8/en0097", "ClueWeb09_English_8/en0098",
			"ClueWeb09_English_8/en0099", "ClueWeb09_English_8/en0100",
			"ClueWeb09_English_8/en0101", "ClueWeb09_English_8/en0102",
			"ClueWeb09_English_8/en0103", "ClueWeb09_English_8/en0104",
			"ClueWeb09_English_8/en0105", "ClueWeb09_English_8/en0106",
			"ClueWeb09_English_8/en0107", "ClueWeb09_English_8/en0108",
			"ClueWeb09_English_8/en0109", "ClueWeb09_English_9/en0110",
			"ClueWeb09_English_9/en0111", "ClueWeb09_English_9/en0112",
			"ClueWeb09_English_9/en0113", "ClueWeb09_English_9/en0114",
			"ClueWeb09_English_9/en0115", "ClueWeb09_English_9/en0116",
			"ClueWeb09_English_9/en0117", "ClueWeb09_English_9/en0118",
			"ClueWeb09_English_9/en0119", "ClueWeb09_English_9/en0120",
			"ClueWeb09_English_9/en0121", "ClueWeb09_English_9/en0122",
			"ClueWeb09_English_9/en0123", "ClueWeb09_English_10/en0124",
			"ClueWeb09_English_10/en0125", "ClueWeb09_English_10/en0126",
			"ClueWeb09_English_10/en0127", "ClueWeb09_English_10/en0128",
			"ClueWeb09_English_10/en0129", "ClueWeb09_English_10/en0130",
			"ClueWeb09_English_10/en0131", "ClueWeb09_English_10/en0132",
			"ClueWeb09_English_10/en0133" };

	private ClueCollectionPathConstants() {
	}

	/**
	 * Adds a sample compressed WARC archive to a Hadoop <code>JobConf</code>
	 * object. The specific archive is
	 * <code>ClueWeb09_English_1/en0000/00.warc.gz</code>, which contains
	 * 35,582 Web pages.
	 * 
	 * @param conf
	 *            Hadoop <code>JobConf</code>
	 * @param base
	 *            base path for the Clue Web collection
	 */
	public static void addEnglishTestFile(JobConf conf, String base) {
		for (String s : sEnglishTest) {
			FileInputFormat.addInputPath(conf, new Path(base + "/" + s));
		}
	}

	/**
	 * Adds the first section of the Clue Web English collection to a Hadoop
	 * <code>JobConf</code> object. Specifically, this method adds the
	 * contents of <code>ClueWeb09_English_1/en0000/</code>, which contains
	 * 3,382,356 pages.
	 * 
	 * @param conf
	 *            Hadoop <code>JobConf</code>
	 * @param base
	 *            base path for the Clue Web collection
	 */
	public static void addEnglishTinyCollection(JobConf conf, String base) {
		for (String s : sEnglishTiny) {
			FileInputFormat.addInputPath(conf, new Path(base + "/" + s));
		}
	}

	/**
	 * Adds the first part (segment) of the Clue Web English collection to a
	 * Hadoop <code>JobConf</code> object. Specifically, this method adds the
	 * contents of <code>ClueWeb09_English_1/</code>, which contains
	 * 50,220,423 pages.
	 * 
	 * @param conf
	 *            Hadoop <code>JobConf</code>
	 * @param base
	 *            base path for the Clue Web collection
	 */
	public static void addEnglishSmallCollection(JobConf conf, String base) {
		for (String s : sEnglishSmall) {
			FileInputFormat.addInputPath(conf, new Path(base + "/" + s));
		}
	}

	/**
	 * Adds the complete Clue Web English collection to a Hadoop
	 * <code>JobConf</code> object. Specifically, this method adds the
	 * contents of <code>ClueWeb09_English_1/</code> through
	 * <code>ClueWeb09_English_10/</code>, which contains 503,903,810 pages.
	 * 
	 * @param conf
	 *            Hadoop <code>JobConf</code>
	 * @param base
	 *            base path for the Clue Web collection
	 */
	public static void addEnglishCompleteCollection(JobConf conf, String base) {
		for (String s : sEnglishComplete) {
			FileInputFormat.addInputPath(conf, new Path(base + "/" + s));
		}
	}

	/**
	 * Adds a part (segment) of the Clue Web English collection to a Hadoop
	 * <code>JobConf</code> object. Part 1 corresponds to the contents of
	 * <code>ClueWeb09_English_1/</code> (i.e., the "small" collection), all
	 * the way through part 10. Note that adding all ten parts is equivalent to
	 * adding the complete English collection.
	 * 
	 * @param conf
	 *            Hadoop <code>JobConf</code>
	 * @param base
	 *            base path for the Clue Web collection
	 */
	public static void addEnglishCollectionPart(JobConf conf, String base, int i) {
	  Preconditions.checkArgument(i >= 1 && i <= 10);

		for (String s : sEnglishSections[i - 1]) {
			FileInputFormat.addInputPath(conf, new Path(base + "/" + s));
		}
	}
}
