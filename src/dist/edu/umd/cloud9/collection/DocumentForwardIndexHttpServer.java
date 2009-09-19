package edu.umd.cloud9.collection;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.ServletHandler;

import edu.umd.cloud9.mapred.NullInputFormat;
import edu.umd.cloud9.mapred.NullMapper;
import edu.umd.cloud9.mapred.NullOutputFormat;

/**
 * <p>
 * Web server for providing access to documents in a collection.
 * </p>
 * 
 * @author Jimmy Lin
 * 
 */
public class DocumentForwardIndexHttpServer {

	private static final Logger sLogger = Logger.getLogger(DocumentForwardIndexHttpServer.class);

	private static DocumentForwardIndex<Indexable> sForwardIndex;
	private static String collectionPath;

	@SuppressWarnings("unchecked")
	private static class Server extends NullMapper {
		public void run(JobConf conf, Reporter reporter) throws IOException {
			int port = 8888;

			collectionPath = conf.get("CollectionPath");
			String indexFile = conf.get("IndexFile");
			String indexClass = conf.get("IndexClass");
			String mappingFile = conf.get("DocnoMappingDataFile");

			sLogger.info("host: " + InetAddress.getLocalHost().toString());
			sLogger.info("port: " + port);
			sLogger.info("forward index: " + indexFile);
			sLogger.info("base path of collection: " + collectionPath);

			try {
				sForwardIndex = (DocumentForwardIndex<Indexable>) Class.forName(indexClass)
						.newInstance();
				sForwardIndex.loadIndex(indexFile, collectionPath, mappingFile);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing forward index!");
			}

			HttpServer server = new HttpServer();
			SocketListener listener = new SocketListener();
			listener.setPort(port);
			server.addListener(listener);

			try {
				HttpContext context = server.getContext("/");
				ServletHandler handler = new ServletHandler();
				handler.addServlet("FetchDocid", "/fetch_docid", FetchDocid.class.getName());
				handler.addServlet("FetchDocno", "/fetch_docno", FetchDocno.class.getName());
				handler.addServlet("Home", "/", Home.class.getName());
				handler.addServlet("Dump", "/dump/*", "org.mortbay.servlet.Dump");

				context.addHandler(handler);

			} catch (Exception e) {
				e.printStackTrace();
			}

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
	public static class Home extends HttpServlet {

		static final long serialVersionUID = 8253865405L;
		static final Random r = new Random();

		protected void doGet(HttpServletRequest httpServletRequest,
				HttpServletResponse httpServletResponse) throws ServletException, IOException {
			httpServletResponse.setContentType("text/html");
			PrintWriter out = httpServletResponse.getWriter();

			out.println("<html><head><title>Collection Access: " + collectionPath
					+ "</title><head>");
			out.println("<body>");

			out.println("<h3>Collection Access: " + collectionPath + "</h3>");

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
	public static class FetchDocid extends HttpServlet {
		static final long serialVersionUID = 3986721097L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("triggered servlet for fetching docids");

			res.setContentType(sForwardIndex.getContentType());

			PrintWriter out = res.getWriter();

			String docid = null;
			if (req.getParameterValues("docid") != null)
				docid = req.getParameterValues("docid")[0];

			Indexable doc = sForwardIndex.getDocument(docid);

			if (doc != null) {
				sLogger.info("fetched: " + doc.getDocid());
				out.print(doc.getContent());
			} else {
				sLogger.info("trapped error fetching " + docid);

				out.print("<html><head><title>Invalid docid!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid doc: " + docid + "</h3>\n");
				out.print("</body></html>\n");
			}

			out.close();
		}

	}

	// this has to be public
	public static class FetchDocno extends HttpServlet {
		static final long serialVersionUID = 5970126341L;

		public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
				IOException {
			doPost(req, res);
		}

		public void doPost(HttpServletRequest req, HttpServletResponse res)
				throws ServletException, IOException {
			sLogger.info("triggered servlet for fetching docids");

			res.setContentType(sForwardIndex.getContentType());

			PrintWriter out = res.getWriter();

			int docno = 0;
			if (req.getParameterValues("docno") != null)
				docno = Integer.parseInt(req.getParameterValues("docno")[0]);

			Indexable doc = sForwardIndex.getDocument(docno);

			if (doc != null) {
				sLogger.info("fetched: " + doc.getDocid() + " = docno " + docno);
				out.print(doc.getContent());
			} else {
				sLogger.info("trapped error fetching " + docno);

				out.print("<html><head><title>Invalid docno!</title><head>\n");
				out.print("<body>\n");
				out.print("<h1>Error!</h1>\n");
				out.print("<h3>Invalid doc: " + docno + "</h3>\n");
				out.print("</body></html>\n");
			}

			out.close();
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out
					.println("usage: [collection-path] [index-file] [index-class] [docno-mapping-data-file]");
			System.exit(-1);
		}

		String collectionPath = args[0];
		String indexFile = args[1];
		String indexClass = args[2];
		String mappingFile = args[3];

		sLogger.info("Launching DocumentForwardIndexHttpServer");
		sLogger.info(" - collection path: " + collectionPath);
		sLogger.info(" - index file: " + indexFile);
		sLogger.info(" - index class: " + indexClass);
		sLogger.info(" - docno mapping data file: " + mappingFile);

		JobConf conf = new JobConf(DocumentForwardIndexHttpServer.class);

		conf.setJobName("ForwardIndexServer:" + collectionPath);

		conf.set("mapred.child.java.opts", "-Xmx1024m");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(NullInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(Server.class);

		conf.set("CollectionPath", collectionPath);
		conf.set("IndexFile", indexFile);
		conf.set("IndexClass", indexClass);
		conf.set("DocnoMappingDataFile", mappingFile);

		JobClient client = new JobClient(conf);
		client.submitJob(conf);
		System.out.println("server started!");
	}
}