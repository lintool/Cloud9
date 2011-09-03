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

package edu.umd.cloud9.webgraph.driver;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import edu.umd.cloud9.collection.DocumentForwardIndex;
import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;
import edu.umd.cloud9.webgraph.data.IndexableAnchorTextForwardIndex;

/**
 * <p>
 * Runs an HTTP server to explore an anchor text/web graph collection. 
 * Command-line arguments are as follows:
 * </p>
 * 
 * <ul>
 * <li>[index-file]: the forward index file for the anchor text collection</li>
 * <li>[docno-mapping]: ClueWeb09 docno mapping file</li>
 * <li>[ClueWeb09-index-files-base]: the path to ClueWeb09 forward indexes</li>
 * </ul>
 * 
 * <p>
 * ClueWeb09 forward indexes should be stored at <code>[ClueWeb09-index-files-base]/findex.en.XX</code>, 
 * where <code>XX</code> is the segment number.
 * </p>
 * 
 * @author Nima Asadi
 * 
 */

@SuppressWarnings("deprecation")
public class ClueWebAnchorTextForwardIndexHttpServer {

	private static final Logger LOG = Logger.getLogger(ClueWebAnchorTextForwardIndexHttpServer.class);
	private static final String SEPARATOR = ",";

	private static final int[] lastDocs = new int[10];
	private static final ArrayList<String> clueweb = new ArrayList<String>();
	
	private static IndexableAnchorTextForwardIndex sForwardIndex;
	private static DocumentForwardIndex<Indexable>[] docForwardIndex;

	@SuppressWarnings("unchecked")
	private static class ServerMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 8888;

			String indexFile = conf.get("IndexFile");
			String mappingFile = conf.get("DocnoMappingDataFile");
			Path tmpPath = new Path(conf.get("TmpPath"));
			String[] cluewebIndexFiles = conf.get("ClueWebIndexFiles").split(SEPARATOR);

			String host = InetAddress.getLocalHost().toString();

			LOG.info("host: " + host);
			LOG.info("port: " + port);
			LOG.info("forward index: " + indexFile);

			FSDataInputStream in = FileSystem.get(conf).open(new Path(indexFile));
			String indexClass = in.readUTF();
			in.close();

			LOG.info("index class: " + indexClass);

			try {
				sForwardIndex = new IndexableAnchorTextForwardIndex(new ClueWarcDocnoMapping());
				sForwardIndex.loadIndex(new Path(indexFile), new Path(mappingFile), FileSystem.get(conf));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing forward index!");
			}
			
			for(String s : cluewebIndexFiles)
				clueweb.add(s.trim());
			
			//opening clueweb forward index files
			docForwardIndex = new DocumentForwardIndex[clueweb.size()];

			for(int i = 0; i < clueweb.size(); i++) {
				in = FileSystem.get(conf).open(new Path(clueweb.get(i)));
				String indexClueWebClass = in.readUTF();
				in.close();
				try {
					docForwardIndex[i] = (DocumentForwardIndex<Indexable>) Class.forName(indexClueWebClass).newInstance();
					docForwardIndex[i].loadIndex(new Path(clueweb.get(i)), new Path(mappingFile), FileSystem.get(conf));
					lastDocs[i] = docForwardIndex[i].getLastDocno();
					
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException("Error initializing forward index!");
				}
			}

			Server server = new Server(port);
			Context root = new Context(server, "/", Context.SESSIONS);
			root.addServlet(new ServletHolder(new FetchDocidServlet()), "/fetch_docid");
			root.addServlet(new ServletHolder(new FetchDocnoServlet()), "/fetch_docno");
			root.addServlet(new ServletHolder(new FetchDocContentServlet()), "/fetch_content");
			root.addServlet(new ServletHolder(new HomeServlet()), "/");

			FSDataOutputStream out = FileSystem.get(conf).create(tmpPath, true);
			out.writeUTF(host);
			out.close();

			try {
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}

			while (true)
				;
		}
	}

	private ClueWebAnchorTextForwardIndexHttpServer() {
	}

	// this has to be public
	public static class HomeServlet extends HttpServlet {

		static final long serialVersionUID = 8253865405L;
		static final Random r = new Random();

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			res.setContentType("text/html");
			PrintWriter out = res.getWriter();

			out.println("<html><head><title>Collection Access: "
					+ sForwardIndex.getCollectionPath() + "</title><head>");
			out.println("<body>");

			out.println("<h3>Collection Access: " + sForwardIndex.getCollectionPath() + "</h3>");

			int firstDocno = sForwardIndex.getFirstDocno();
			int lastDocno = sForwardIndex.getLastDocno();
			int numDocs = lastDocno - firstDocno;

			String firstDocid = sForwardIndex.getDocid(firstDocno);
			String lastDocid = sForwardIndex.getDocid(lastDocno);

			out.println("First document: docno <a href=\"/fetch_docno?docno=" + firstDocno + "\">"
					+ firstDocno + "</a> or <a href=\"/fetch_docid?docid=" + firstDocid + "\">"
					+ firstDocid + "</a><br/>");
			out.println("Last document: docno <a href=\"/fetch_docno?docno=" + lastDocno + "\">"
					+ lastDocno + "</a> or <a href=\"/fetch_docid?docid=" + lastDocid + "\">"
					+ lastDocid + "</a>");

			out.println("<h3>Fetch a docid</h3>");

			String id;

			out.println("<p>(random examples: ");

			id = sForwardIndex.getDocid(r.nextInt(numDocs) + firstDocno);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

			id = sForwardIndex.getDocid(r.nextInt(numDocs) + firstDocno);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

			id = sForwardIndex.getDocid(r.nextInt(numDocs) + firstDocno);
			out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>)</p>");

			out.println("<form method=\"post\" action=\"fetch_docid\">");
			out.println("<input type=\"text\" name=\"docid\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch!\" />");
			out.println("</form>");
			out.println("</p>");
			
			out.println("<p>");
			out.println("<form method=\"post\" action=\"fetch_content\">");
			out.println("<input type=\"text\" name=\"docid\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch Content!\" />");
			out.println("</form>");
			out.println("</p>");

			out.println("<h3>Fetch a docno</h3>");

			int n;
			out.println("<p>(random examples: ");

			n = r.nextInt(numDocs) + firstDocno;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>, ");

			n = r.nextInt(numDocs) + firstDocno;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>, ");

			n = r.nextInt(numDocs) + firstDocno;
			out.println("<a href=\"/fetch_docno?docno=" + n + "\">" + n + "</a>)</p>");

			out.println("<p>");
			out.println("<form method=\"post\" action=\"fetch_docno\">");
			out.println("<input type=\"text\" name=\"docno\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch!\" />");
			out.println("</form>");
			out.println("</p>");
			
			out.println("<p>");
			out.println("<form method=\"post\" action=\"fetch_content\">");
			out.println("<input type=\"text\" name=\"docno\" size=\"60\" />");
			out.println("<input type=\"submit\" value=\"Fetch Content!\" />");
			out.println("</form>");
			out.println("</p>");

			out.print("</body></html>\n");

			out.close();
		}
	}
	
	public static class FetchDocContentServlet extends HttpServlet {
		static final long serialVersionUID = 5970126341L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			LOG.info("triggered servlet for fetching document content");

			int docno = 0;
			try {
				if (req.getParameterValues("docno") != null)
					docno = Integer.parseInt(req.getParameterValues("docno")[0]);
				else if (req.getParameterValues("docid") != null)
					docno = sForwardIndex.getDocno(req.getParameterValues("docid")[0]);

				Indexable doc = null;
				int i = 0;
				for(i = 0; i < lastDocs.length; i++)
					if(docno <= lastDocs[i]) {
						doc = docForwardIndex[i].getDocument(docno);
						break;
					}

				if (doc != null) {
					LOG.info("fetched: " + doc.getDocid() + " = docno " + docno);
					res.setContentType(doc.getDisplayContentType());

					PrintWriter out = res.getWriter();
					out.print(doc.getContent().replaceAll("<\\s*/\\s*[bB][oO][dD][Yy]\\s*>", "<br><br><a href=\"/fetch_docno?docno=" + docno + 
								"\"> Fetch anchor text for docno: " + docno + "</a></body>"));
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				LOG.info("trapped error fetching " + docno);
				res.setContentType("text/html");

				PrintWriter out = res.getWriter();
				out.print("<html><head><title>Invalid docno!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid docno: " + docno + "</h3>\n");
				out.print("</body></html>\n");
				out.close();
			}
		}
	}

	// this has to be public
	public static class FetchDocidServlet extends HttpServlet {
		static final long serialVersionUID = 3986721097L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			LOG.info("triggered servlet for fetching document by docid");
			String docid = null;

			try {
				
				if (req.getParameterValues("docid") != null)
					docid = req.getParameterValues("docid")[0];

				Indexable doc = sForwardIndex.getDocument(docid);
				
				if (doc != null) {
					LOG.info("fetched: " + doc.getDocid());
					res.setContentType(doc.getDisplayContentType());

					PrintWriter out = res.getWriter();
					out.print(doc.getContent().replace("<body>", "<body><a href=\"/fetch_content?docid=" + docid + 
							"\"> Fetch content for docid: " + docid + "</a><br><br>"));
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				// catch-all, in case anything goes wrong
				LOG.info("trapped error fetching " + docid);
				res.setContentType("text/html");

				PrintWriter out = res.getWriter();
				out.print("<html><head><title>Invalid docid!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid docid: " + docid + "</h3>\n");
				out.print("</body></html>\n");
				out.close();
			}
		}

	}

	// this has to be public
	public static class FetchDocnoServlet extends HttpServlet {
		static final long serialVersionUID = 5970126341L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			LOG.info("triggered servlet for fetching document by docno");

			int docno = 0;
			try {
				
				if (req.getParameterValues("docno") != null)
					docno = Integer.parseInt(req.getParameterValues("docno")[0]);

				Indexable doc = sForwardIndex.getDocument(docno);
	
				if (doc != null) {
					LOG.info("fetched: " + doc.getDocid() + " = docno " + docno);
					res.setContentType(doc.getDisplayContentType());
					
					PrintWriter out = res.getWriter();
					out.print(doc.getContent().replace("<body>", "<body><a href=\"/fetch_content?docno=" + docno + 
							"\"> Fetch content for docno: " + docno + "</a><br><br>"));
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				LOG.info("trapped error fetching " + docno);
				res.setContentType("text/html");

				PrintWriter out = res.getWriter();
				out.print("<html><head><title>Invalid docno!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid docno: " + docno + "</h3>\n");
				out.print("</body></html>\n");
				out.close();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.out.println("usage: [index-file] [docno-mapping-data-files] [clue-forward-index-root]");
			//[clue-forward-index-root: /shared/ClueWeb09/collection.compressed.block/
			System.exit(-1);
		}

		String indexFile = otherArgs[0];
		String mappingFile = otherArgs[1];
		String clueIndexRoot = otherArgs[2].endsWith("/") ? otherArgs[2] : otherArgs[2] + "/";
		
		String cluewebForwardIndex = "";
		for(int i = 1; i < 10; i++)
			cluewebForwardIndex += clueIndexRoot + "findex.en.0" + i + ".dat" + SEPARATOR + " ";
		cluewebForwardIndex += clueIndexRoot + "findex.en.10.dat";

		LOG.info("Launching DocumentForwardIndexHttpServer");
		LOG.info(" - index file: " + indexFile);
		LOG.info(" - docno mapping data file: " + mappingFile);
		LOG.info(" - ClueWeb09 index root:" + clueIndexRoot);

		FileSystem fs = FileSystem.get(conf);

		Random rand = new Random();
		int r = rand.nextInt();
		
		// this tmp file as a rendezvous point
		Path tmpPath = new Path("/tmp/" + r);

		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

		JobConf job = new JobConf(conf, ClueWebAnchorTextForwardIndexHttpServer.class);

		job.setJobName("ForwardIndexServer:" + indexFile);
		
		job.set("mapred.child.java.opts", "-Xmx2048m");

		job.setNumMapTasks(1);
		job.setNumReduceTasks(0);

		job.setInputFormat(NullInputFormat.class);
		job.setOutputFormat(NullOutputFormat.class);
		job.setMapperClass(ServerMapper.class);

		job.set("IndexFile", indexFile);
		job.set("DocnoMappingDataFile", mappingFile);
		job.set("TmpPath", tmpPath.toString());
		job.set("ClueWebIndexFiles", cluewebForwardIndex);

		JobClient client = new JobClient(job);
		client.submitJob(job);

		LOG.info("Waiting for server to start up...");

		while (!fs.exists(tmpPath)) {
			Thread.sleep(50000);
			LOG.info("...");
		}

		FSDataInputStream in = fs.open(tmpPath);
		String host = in.readUTF();
		in.close();

		LOG.info("host: " + host);
		LOG.info("port: 8888");
	}
}