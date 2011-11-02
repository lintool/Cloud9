package edu.umd.cloud9.webgraph.driver.wt10g;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.collection.DocnoMapping;

public class AgencyGenericDocnoMapping implements DocnoMapping
{
	//Abandoned

    DocnoMapping agent;

//    AgencyGenericDocnoMapping(String input) throws Exception
//    {
//	super();
//
//	CollectionConfigurationManager tmp = new CollectionConfigurationManager();
//	// user specified class
//	if (input.endsWith(".class"))
//	{
//	    tmp.setUserSpecifiedDocnoMappingClass(input);
//	    agent = tmp.generateDocmapping(null);//TODO we should pass in conf.......
//	}
//	else
//	{
//	    tmp.setConfByCollection(input);
//	    agent = tmp.generateDocmapping(null);
//	}
//    }

    @Override
    public int getDocno(String docid)
    {
	// TODO Auto-generated method stub
	return agent.getDocno(docid);
    }

    @Override
    public String getDocid(int docno)
    {
	// TODO Auto-generated method stub
	return agent.getDocid(docno);
    }

    @Override
    public void loadMapping(Path path, FileSystem fs) throws IOException
    {
	// TODO Auto-generated method stub
	agent.loadMapping(path, fs);
    }

}
