package edu.umd.hooka;

//Copied from http://www.javapractices.com/topic/TopicAction.do?Id=68 

import java.util.*;
import java.io.*;


public final class FileListing {

	  /**
	  * Demonstrate use.
	  * 
	  * @param aArgs - <tt>aArgs[0]</tt> is the full name of an existing directory,
	  * that can be read
	  */
	  public static void main(String... aArgs) throws FileNotFoundException {
	    File tempDir = new File(aArgs[0]);
	    List<File> files = FileListing.getFileListing( tempDir );

	    //print out all file names, and display the order of File.compareTo
	    for(File file : files ){
	      System.out.println(file);
	    }
	  }

	  /**
	  * Recursively walk a directory tree and return a List of all
	  * Files found; the List is sorted using File.compareTo.
	  *
	  * @param aStartingDir is a valid directory, which can be read.
	  */
	  static public List<File> getFileListing(
	    File aStartingDir
	  ) throws FileNotFoundException {
	    validateDirectory(aStartingDir);
	    List<File> result = new ArrayList<File>();

	    File[] filesAndDirs = aStartingDir.listFiles();
	    List<File> filesDirs = Arrays.asList(filesAndDirs);
	    for(File file : filesDirs) {
	      if(file.isFile())
	    	  result.add(file); //add file
	      else{
	        //must be a directory
	        //recursive call!
	        List<File> deeperList = getFileListing(file);
	        result.addAll(deeperList);
	      }

	    }
	    Collections.sort(result);
	    return result;
	  }

	  /**
	  * Directory is valid if it exists, does not represent a file, and can be read.
	  */
	  static private void validateDirectory (
	    File aDirectory
	  ) throws FileNotFoundException {
	    if (aDirectory == null) {
	      throw new IllegalArgumentException("Directory should not be null.");
	    }
	    if (!aDirectory.exists()) {
	      throw new FileNotFoundException("Directory does not exist: " + aDirectory);
	    }
	    if (!aDirectory.isDirectory()) {
	      throw new IllegalArgumentException("Is not a directory: " + aDirectory);
	    }
	    if (!aDirectory.canRead()) {
	      throw new IllegalArgumentException("Directory cannot be read: " + aDirectory);
	    }
	  }
	} 
