package edu.umd.cloud9.collection;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
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

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * <p>
 * Web server for providing access to documents in a collection. Sample
 * command-line invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.DocumentForwardIndexHttpServer \
 *   /shared/ClueWeb09/collection.compressed.block/findex.en.01.dat \
 *   /shared/ClueWeb09/docno-mapping.dat
 * </pre>
 * 
 * @author Jimmy Lin
 * 
 */
public class DocumentForwardIndexHttpServer {

	private static final Logger sLogger = Logger.getLogger(DocumentForwardIndexHttpServer.class);

	private static DocumentForwardIndex<Indexable> sForwardIndex;

	@SuppressWarnings("unchecked")
	private static class ServerMapper extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 8888;

			String indexFile = conf.get("IndexFile");
			String mappingFile = conf.get("DocnoMappingDataFile");
			Path tmpPath = new Path(conf.get("TmpPath"));

			String host = InetAddress.getLocalHost().toString();

			sLogger.info("host: " + host);
			sLogger.info("port: " + port);
			sLogger.info("forward index: " + indexFile);

			FSDataInputStream in = FileSystem.get(conf).open(new Path(indexFile));
			String indexClass = in.readUTF();
			in.close();

			sLogger.info("index class: " + indexClass);

			try {
				sForwardIndex = (DocumentForwardIndex<Indexable>) Class.forName(indexClass)
						.newInstance();
				sForwardIndex.loadIndex(indexFile, mappingFile);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing forward index!");
			}

			Server server = new Server(port);
			Context root = new Context(server, "/", Context.SESSIONS);
			root.addServlet(new ServletHolder(new FetchDocidServlet()), "/fetch_docid");
			root.addServlet(new ServletHolder(new FetchDocnoServlet()), "/fetch_docno");
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

	private DocumentForwardIndexHttpServer() {
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

			out.print("</body></html>\n");

			out.close();
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
			sLogger.info("triggered servlet for fetching document by docid");
			String docid = null;

			try {
				if (req.getParameterValues("docid") != null)
					docid = req.getParameterValues("docid")[0];

				Indexable doc = sForwardIndex.getDocument(docid);

				if (doc != null) {
					sLogger.info("fetched: " + doc.getDocid());
					res.setContentType(doc.getDisplayContentType());

					PrintWriter out = res.getWriter();
					out.print(doc.getDisplayContent());
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				// catch-all, in case anything goes wrong
				sLogger.info("trapped error fetching " + docid);
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
			sLogger.info("triggered servlet for fetching document by docno");

			int docno = 0;
			try {
				if (req.getParameterValues("docno") != null)
					docno = Integer.parseInt(req.getParameterValues("docno")[0]);

				Indexable doc = sForwardIndex.getDocument(docno);

				if (doc != null) {
					sLogger.info("fetched: " + doc.getDocid() + " = docno " + docno);
					res.setContentType(doc.getDisplayContentType());

					PrintWriter out = res.getWriter();
					out.print(doc.getDisplayContent());
					out.close();
				} else {
					throw new Exception();
				}
			} catch (Exception e) {
				sLogger.info("trapped error fetching " + docno);
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

	// TODO: this should probably be made into a "Tool"
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("usage: [index-file] [docno-mapping-data-file]");
			System.exit(-1);
		}

		String indexFile = otherArgs[0];
		String mappingFile = otherArgs[1];

		sLogger.info("Launching DocumentForwardIndexHttpServer");
		sLogger.info(" - index file: " + indexFile);
		sLogger.info(" - docno mapping data file: " + mappingFile);

		FileSystem fs = FileSystem.get(conf);

		Random rand = new Random();
		int r = rand.nextInt();
		
		// this tmp file as a rendezvous point
		Path tmpPath = new Path("/tmp/" + r);

		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

		JobConf job = new JobConf(conf, DocumentForwardIndexHttpServer.class);

		job.setJobName("ForwardIndexServer:" + indexFile);

		job.set("mapred.child.java.opts", "-Xmx1024m");

		job.setNumMapTasks(1);
		job.setNumReduceTasks(0);

		job.setInputFormat(NullInputFormat.class);
		job.setOutputFormat(NullOutputFormat.class);
		job.setMapperClass(ServerMapper.class);

		job.set("IndexFile", indexFile);
		job.set("DocnoMappingDataFile", mappingFile);
		job.set("TmpPath", tmpPath.toString());

		JobClient client = new JobClient(job);
		client.submitJob(job);

		sLogger.info("Waiting for server to start up...");

		while (!fs.exists(tmpPath)) {
			Thread.sleep(50000);
			sLogger.info("...");
		}

		FSDataInputStream in = fs.open(tmpPath);
		String host = in.readUTF();
		in.close();

		sLogger.info("host: " + host);
		sLogger.info("port: 8888");
	}
}