package edu.umd.hooka;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Collection;

public class ProfileLogParser {

	static final int JOB_START = 0;
	static final int MAP_START = 1;
	static final int MAP_FINISH = 2;
	static final int REDUCE_START = 3;
	static final int REDUCE_FINISH = 4;
	static final int JOB_FINISH = 5;
	static final int INVALID = 6;

	private class nodeData
	{
		long mapStart;
		long mapFinish;
		long reduceStart;
		long reduceFinish;
	}
	
	private class itemData
	{
		String identifier;
		int itemType;
		long timestamp;
		
		public itemData(String data)
		{
			StringTokenizer tokens = new StringTokenizer(data);
			String currToken = "";
			try{
			while(!currToken.endsWith(":"))
				currToken = tokens.nextToken();
			//Item type
			currToken = tokens.nextToken();
			if (currToken.equals("JOB_START")) itemType = JOB_START;
			else if(currToken.equals("MAP_START")) itemType = MAP_START;
			else if (currToken.equals("MAP_FINISH")) itemType = MAP_FINISH;
			else if (currToken.equals("REDUCE_START")) itemType = REDUCE_START;
			else if (currToken.equals("REDUCE_FINISH")) itemType = REDUCE_FINISH;
			else if (currToken.equals("JOB_FINISH")) itemType = JOB_FINISH;
			else itemType = INVALID;
			//Identifier
			currToken = tokens.nextToken();
			identifier = currToken;
			//timestamp
			currToken = tokens.nextToken();
			timestamp = Long.parseLong(currToken);
			}
			catch(NoSuchElementException e)
			{
				itemType = INVALID;
			}
			catch(NumberFormatException e)
			{
				itemType = INVALID;
			}
		}
	}
	
	public void Parse(Collection<File> theFiles, long startTime, long finishTime, OutputStream output) throws IOException
	{
		BufferedReader inputReader = null;
		BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(output));
		HashMap<String, nodeData> dataHashMap = new HashMap<String, nodeData>();
		itemData currData = null;
		nodeData currNodeData = null;
		long jobStartTime = startTime;
		long jobFinishTime = finishTime;
		String currLine = null;
		for(File currFile : theFiles)
		{
			try{
				inputReader = new BufferedReader(new InputStreamReader(new FileInputStream(currFile)));
			}
			catch(FileNotFoundException e)
			{
				System.err.println(e.getMessage());
				e.printStackTrace();
				System.err.println("Input file not found");
				return;
			}
			currLine = inputReader.readLine();
			while(currLine != null)
			{
				currData = new itemData(currLine);
				if(currData.itemType == INVALID)
				{
					currLine = inputReader.readLine();
					continue;
				}
				if(currData.itemType == JOB_START)
					jobStartTime = currData.timestamp;
				else if (currData.itemType == JOB_FINISH)
					jobFinishTime = currData.timestamp;
				else
				{
					if(!(dataHashMap.containsKey(currData.identifier)))
						dataHashMap.put(currData.identifier, new nodeData());
					currNodeData = dataHashMap.get(currData.identifier);
					switch(currData.itemType)
					{
					case MAP_START: currNodeData.mapStart = currData.timestamp;
					break;
					case MAP_FINISH: currNodeData.mapFinish = currData.timestamp;
					break;
					case REDUCE_START: currNodeData.reduceStart = currData.timestamp;
					break;
					case REDUCE_FINISH: currNodeData.reduceFinish = currData.timestamp;
					break;
					}
				}
				currLine = inputReader.readLine();
			}
		}
		//Output the information
		int numNodes = 0;
		int failedReports = 0;
		long timeTaken = jobFinishTime - jobStartTime;
		long preMap = 0L;
		long mapTime = 0L;
		long intermediate = 0L;
		long reduceTime = 0L;
		long postReduce = 0L;
		for(nodeData x : dataHashMap.values())
		{
			if((x.mapStart == 0) || (x.mapFinish == 0) || (x.reduceStart == 0) || (x.reduceFinish == 0))
				failedReports += 1;
			else
			{
				preMap += (x.mapStart - jobStartTime);
				mapTime += (x.mapFinish - x.mapStart);
				intermediate += (x.reduceStart - x.mapFinish);
				reduceTime += (x.reduceFinish - x.reduceStart);
				postReduce += (jobFinishTime - x.reduceFinish);
				numNodes += 1;
			}
		}
		outputWriter.write(Integer.toString(numNodes) + " total nodes reporting for " + Long.toString(timeTaken) + " milliseconds each\n");
		outputWriter.write("Total node time taken before map operations: " + Long.toString(preMap) + "\n");
		outputWriter.write("Total node time taken for map operations: " + Long.toString(mapTime) + "\n");
		outputWriter.write("Total node time taken between map and reduce operations: " + Long.toString(intermediate) + "\n");
		outputWriter.write("Total node time taken for reduce operations: " + Long.toString(reduceTime) + "\n");
		outputWriter.write("Total node time taken after reduce operations: " + Long.toString(postReduce) + "\n");
		outputWriter.write(Integer.toString(failedReports) + " nodes reporting incomplete data (not counted in above) \n");
		outputWriter.close();	
	}
	
	public static void main(String[] args)
	{
		String directory = "/home/guest/hadoop/logs/userlogs/thistask";
		long startTime = 1205673885264L;
		long finishTime = 1205673915411L;
		Collection<File> theFiles = null;
		try{theFiles = FileListing.getFileListing(new File(directory));}
		catch(FileNotFoundException e){e.printStackTrace();}
		try{new ProfileLogParser().Parse(theFiles, startTime, finishTime, System.out);}
		catch(IOException e){e.printStackTrace();}
	}
}
