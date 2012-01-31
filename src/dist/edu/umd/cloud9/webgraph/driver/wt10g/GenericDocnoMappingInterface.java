package edu.umd.cloud9.webgraph.driver.wt10g;

import org.apache.hadoop.conf.Configuration;

import edu.umd.cloud9.collection.DocnoMapping;

public interface GenericDocnoMappingInterface extends DocnoMapping
{
    //Return required fields in conf
    public String[] requiredConf();
    
    //This function will be called in GenericExtractLinks before it is used to convert docno
    //Exception should be thrown if input file name is not set in conf
    //We recommend error message is printed in screen also.
    public void preConfig(Configuration conf) throws Exception;
}