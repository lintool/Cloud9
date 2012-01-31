package edu.umd.cloud9.webgraph.driver.wt10g;

import java.util.ArrayList;

public class ArgumentManager
{
    ArrayList<String> args = new ArrayList<String>();
    
    public void insertArg(String argument)
    {
	args.add(argument);
    }
    
    public boolean hasNext(int num)
    {
	return (args.size()>=num);
    }
    
    public String getNext()
    {
	String tmp = args.get(0);
	args.remove(0);
	return tmp;
    }
}
