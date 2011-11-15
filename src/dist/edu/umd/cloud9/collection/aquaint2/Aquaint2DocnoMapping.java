/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.collection.aquaint2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.DocnoMapping;

public class Aquaint2DocnoMapping implements DocnoMapping {
  private static final Logger LOG = Logger.getLogger(Aquaint2DocnoMapping.class);
  {
    LOG.setLevel(Level.INFO);
    //LOG.setLevel(Level.TRACE);
  }

  private String[] docidEntries;

  @Override
  public int getDocno(String docid) {
    LOG.trace("getDocno(docid: " + docid + ")");
    Preconditions.checkNotNull(docid);
    int sourceLength = docid.length() - 13;
    String source = docid.substring(0, sourceLength);
    int year = Integer.parseInt(docid.substring (sourceLength, sourceLength + 4));
    int month = Integer.parseInt(docid.substring (sourceLength + 4, sourceLength + 6));
    int day = Integer.parseInt(docid.substring (sourceLength + 6, sourceLength + 8));
    int articleNo = Integer.parseInt(docid.substring (sourceLength + 9, sourceLength + 13));


    // first traverse the entries to find the month entry and get its days
    int entryId = findEntryId(source, year, month);
    LOG.debug("entryId: " + entryId);

    String entryElt = docidEntries[entryId].split("\t")[day];
    LOG.debug("entryElt: " + entryElt);

    // then traverse the days to find the day and skip over missing articles to get the article number
    String[] entryEltParts = entryElt.split(" ");
    int result = articleNo + Integer.parseInt(entryEltParts[0]);
    String[] entryDayParts = entryEltParts[1].split(",");
    for (int i = 1; i < entryDayParts.length; i++) {
      int missingNo = Integer.parseInt(entryDayParts[i]);
      if (articleNo < missingNo) break;
      LOG.debug("skipping missingNo: " + missingNo);
      result--;
    }

    LOG.trace("getDocno returning: " + result);
    return result;
  }

  private int findEntryId(String source, int year, int month) {
    for (int i = 0; i < docidEntries.length; i++) {
      LOG.debug("docidEntries [" + i + "]: " + docidEntries[i]);
      String[] entryElts = docidEntries[i].split("\t");
      String[] entryMetaInfo = entryElts[0].split(" ");
      String entrySource = entryMetaInfo[1];
      if (entrySource.equals (source)) {
        int entryYear = Integer.parseInt(entryMetaInfo[2]);
        if (entryYear == year) {
          int entryMonth = Integer.parseInt(entryMetaInfo[3]);
          if (entryMonth == month) {
            return i;
          }
        }
      }
    }
    return -1;
  }


  @Override
  public String getDocid(int docno) {
    Preconditions.checkArgument(docno > 0);
    LOG.trace("getDocid(docno: " + docno + ")");

    // first traverse the entries to find the month entry and get its source, year, month
    int entryId = findEntryId(docno);
    LOG.debug("entryId: " + entryId);
    String[] entryElts = docidEntries[entryId].split("\t");
    String[] entryMetaInfo = entryElts[0].split(" ");
    String source = entryMetaInfo[1];
    int year = Integer.parseInt(entryMetaInfo[2]);
    int month = Integer.parseInt(entryMetaInfo[3]);
    LOG.debug("looking at: " + String.format("%s%04d%02d__.____", source, year, month));

    // then traverse the days to find the day and skip over missing articles to get the article number
    String[] entryEltParts = findEntryEltParts (docno, entryElts);
    int offset = Integer.parseInt(entryEltParts[0]);
    String[] entryDayParts = entryEltParts[1].split(",");
    int day = Integer.parseInt(entryDayParts[0]);
    LOG.debug("found day: " + day + ", looking at: " + String.format("%s%04d%02d%02d.____", source, year, month, day));
    int articleNo = docno - offset;
    for (int i = 1; i < entryDayParts.length; i++) {
      int missingNo = Integer.parseInt(entryDayParts[i]);
      if (articleNo < missingNo) break;
      LOG.debug("skipping missingNo: " + missingNo);
      articleNo++;
    }
    LOG.debug("found articleNo: " + articleNo + ", looking at: " + String.format("%s%04d%02d%02d.%04d", source, year, month, day, articleNo));
    String result = String.format ("%s%04d%02d%02d.%04d", source, year, month, day, articleNo);
    LOG.trace("getDocid returning: " + result);
    return result;
  }


  private int findEntryId(int docno) {
    for (int i = 0; i < docidEntries.length; i++) {
      LOG.debug("docidEntries [" + i + "]: " + docidEntries[i]);
      int entryOffset = Integer.parseInt(docidEntries[i].split(" ") [0]);
      if (entryOffset >= docno) {
        return i - 1;
      }
    }
    return docidEntries.length - 1;
  }


  private String[] findEntryEltParts(int docno, String[] entryElts) {
    String[] thisEltParts = new String[0];
    int prevOffset = -1;
    String[] prevEltParts = new String[0];

    for (int i = 1; i < entryElts.length; i++) {
      thisEltParts = entryElts[i].split(" ");
      int thisOffset = Integer.parseInt(thisEltParts[0]);
      if (thisOffset >= docno) {
        return prevEltParts;
      }
      prevOffset = thisOffset;
      prevEltParts = thisEltParts;
    }
    return thisEltParts;
  }


  @Override
  public void loadMapping(Path p, FileSystem fs) throws IOException {
    docidEntries = Aquaint2DocnoMapping.readDocnoData(p, fs);
  }


  static public void writeDocnoData(Path input, Path output, FileSystem fs) throws IOException {
    LOG.info("Writing docno data to " + output);
    LineReader reader = new LineReader(fs.open(input));
    List<String> list = Lists.newArrayList();

    LOG.info("Reading " + input);
    int cnt = 0;
    Text line = new Text();

    String prevSource = null;
    int prevYear = -1;
    int prevMonth = -1;
    int prevDay = -1;
    int prevArticleNo = -1;
    StringBuilder currentEntry = null;
    int numEntries = 0;

    while (reader.readLine(line) > 0) {
      String docid = line.toString();

      int sourceLength = docid.indexOf("\t") - 13;
      String source = docid.substring(0, sourceLength);
      int year = Integer.parseInt(docid.substring (sourceLength, sourceLength + 4));
      int month = Integer.parseInt(docid.substring (sourceLength + 4, sourceLength + 6));
      int day = Integer.parseInt(docid.substring (sourceLength + 6, sourceLength + 8));
      int articleNo = Integer.parseInt(docid.substring (sourceLength + 9, sourceLength + 13));
      LOG.debug("prevSource: " + prevSource + ", prevYear: " + prevYear + ", prevMonth: " + prevMonth + ", prevDay: " + prevDay + ", prevArticleNo: " + prevArticleNo);
      LOG.debug("source: " + source + ", year: " + year + ", month: " + month + ", day: " + day + ", articleNo: " + articleNo);

      if (! source.equals(prevSource) ||
           year != prevYear ||
           month != prevMonth) {
        LOG.debug("currentEntry: " + currentEntry);
        if (currentEntry != null) {
          list.add(currentEntry.toString());
          list.add("<eol>");
          numEntries++;
        }
        currentEntry = new StringBuilder(cnt + " " + source + " " + year + " " + month);
        prevDay = 0;
        prevArticleNo = 0;
      }
      if (day != prevDay) {
        for (int i = prevDay + 1; i <= day; i++) {
          currentEntry.append("\t" + cnt + " " + i);
        }
        prevArticleNo = 0;
        // writeUTF can't write a string longer than 64k, so we output a chunk at a time
        // here then concatenate strings between <eol>s
        list.add(currentEntry.toString());
        currentEntry = new StringBuilder ();
      }
      if (articleNo != prevArticleNo + 1) {
        // we have missing article numbers - gather them
        for (int i = prevArticleNo + 1; i < articleNo; i++) {
          currentEntry.append("," + i);
        }
      }
      prevSource = source;
      prevYear = year;
      prevMonth = month;
      prevDay = day;
      prevArticleNo = articleNo;

      cnt++;
      if (cnt % (200 * 1000) == 0) {
        LOG.info(cnt + " docs");
      }
    }
    list.add(currentEntry.toString());
    list.add("<eof>");
    numEntries++;
    list.add("" + cnt);
    reader.close();
    LOG.info(cnt + " docs total. Done!");

    LOG.info("Writing " + output);
    FSDataOutputStream out = fs.create(output, true);
    out.writeInt(numEntries);
    numEntries = 0;
    for (int i = 0; i < list.size(); i++) {
      out.writeUTF(list.get(i));
      numEntries++;
      if (numEntries % (10 * 1000) == 0) {
        LOG.info(numEntries + " months of docs");
      }
    }
    out.close();
    LOG.info(numEntries + " months of docs total. Done!");
  }

  static public String[] readDocnoData(Path p, FileSystem fs) throws IOException {
    LOG.trace("readDocnoData (p: " + p + ", fs)");
    FSDataInputStream in = fs.open(p);

    int sz = in.readInt();
    LOG.debug("creating a month array of length: " + sz);
    String[] arr = new String[sz];
    String currentEntryPart = in.readUTF();
    StringBuilder currentEntry = new StringBuilder();
    int i = 0;
    while (!currentEntryPart.equals("<eof>")) {
      LOG.debug("currentEntryPart: " + currentEntryPart);
      if (currentEntryPart.equals("<eol>")) {
        arr[i] = currentEntry.toString();
        LOG.debug("arr[" + i + "]: " + arr[i]);
        i++;
        currentEntry = new StringBuilder();
      } else {
        currentEntry.append(currentEntryPart);
      }
      currentEntryPart = in.readUTF();
    }

    if (currentEntry.length() > 0){
      arr[i] = currentEntry.toString();
      LOG.debug("arr[" + i + "]: " + arr[i]);
    }
    in.close();

    return arr;
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path mappingPath = new Path(args[1]);
    System.out.println("loading mapping file " + mappingPath);
    Aquaint2DocnoMapping mapping = new Aquaint2DocnoMapping();
    mapping.loadMapping(mappingPath, fs);

    if (args[0].equals("list")) {
      for (int i = 1; i < mapping.docidEntries.length; i++) {
        System.out.println(i + "\t" + mapping.docidEntries[i]);
      }
    } else if (args[0].equals("getDocno")) {
      System.out.println("looking up docno for \"" + args[2] + "\"");
      int idx = mapping.getDocno(args[2]);
      if (idx > 0) {
        System.out.println(mapping.getDocno(args[2]));
      } else {
        System.err.print("Invalid docid!");
      }
    } else if (args[0].equals("getDocid")) {
      try {
        System.out.println("looking up docid for " + args[2]);
        System.out.println(mapping.getDocid(Integer.parseInt(args[2])));
      } catch (Exception e) {
        System.err.print("Invalid docno!");
      }
    } else {
      System.out.println("Invalid command!");
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
    }
  }
}