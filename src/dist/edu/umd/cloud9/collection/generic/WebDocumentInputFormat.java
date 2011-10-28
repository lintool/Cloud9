package edu.umd.cloud9.collection.generic;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat;

public class WebDocumentInputFormat extends
        FileInputFormat<LongWritable, WebDocument>
{
    public static final String[] supported = { "clue", "trecweb", "trec",
	    "wt10g", "gov2" };// if one collection's name contain name of
			      // another in beginning, it should appear first.
    public FileInputFormat fileFormatAgent = null;

    public static boolean isSupport(String tgtCollection)
    {
	return (getSupportIndex(tgtCollection) != -1);
    }

    private static int getSupportIndex(String tgtCollection)
    {
	tgtCollection = tgtCollection.toLowerCase();
	for (int i = 0; i < supported.length; i++)
	    if (tgtCollection.startsWith(supported[i]))
		return i;
	return -1;
    }

    @Override
    public RecordReader<LongWritable, WebDocument> getRecordReader(
	    InputSplit arg0, JobConf arg1, Reporter arg2) throws IOException
    {
	if (fileFormatAgent == null)
	    try
            {
	        buildAgent(arg1);
            }
            catch (Exception e)
            {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
            }
	return fileFormatAgent.getRecordReader(arg0, arg1, arg2);
    }

    private void buildAgent(JobConf conf) throws Exception
    {
	String inputString = conf.get("inputString", "");
	boolean userSpecifiedClass = conf.getBoolean("userSpecifiedClass", false);	
	
	if(userSpecifiedClass)
	{
	    Object obj = Class.forName(inputString).getInterfaces();
	    if(!FileInputFormat.class.isInstance(obj))
		throw new Exception(String.format("Specified class '%s' has to be sub-class of FileInputFormat",inputString));
	    fileFormatAgent = (FileInputFormat)obj;
	    return;
	}
	
	int index = getSupportIndex(inputString);
	switch(index)
	{
	    case 0:
		fileFormatAgent = new ClueWarcInputFormat();
		break;
	    case 1:case 3: case 4:
		fileFormatAgent = new TrecWebDocumentInputFormat();
		break;
	    case 2:
		fileFormatAgent = new TrecDocumentInputFormat();
		break;
	    default:
		throw new Exception(String.format("Target collection '%s'not supported.",inputString));
	}
    }
}
