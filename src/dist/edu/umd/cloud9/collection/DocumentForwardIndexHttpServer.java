package edu.umd.cloud9.collection;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;

import edu.umd.cloud9.mapreduce.NullInputFormat;
import edu.umd.cloud9.mapreduce.NullMapper;

/**
 * Web server for providing access to documents in a collection.
 *
 * @author Jimmy Lin
 */
public class DocumentForwardIndexHttpServer extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DocumentForwardIndexHttpServer.class);
  private static DocumentForwardIndex<Indexable> INDEX;

  // Keys for passing data into mapper via conf object.
  private static final String INDEX_KEY = "index";
  private static final String DOCNO_MAPPING_KEY = "docnoMapping";
  private static final String TMP_KEY = "tmp";

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static class MyMapper extends NullMapper {
    @Override
    public void runSafely(Mapper.Context context) {
      try {
        int port = 8888;

        Configuration conf = context.getConfiguration();
        String indexFile = conf.get(INDEX_KEY);
        String mappingFile = conf.get(DOCNO_MAPPING_KEY);
        Path tmpPath = new Path(conf.get(TMP_KEY));

        String host = InetAddress.getLocalHost().toString();

        LOG.info("host: " + host);
        LOG.info("port: " + port);
        LOG.info("forward index: " + indexFile);

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(indexFile));
        String indexClass = in.readUTF();
        in.close();

        LOG.info("index class: " + indexClass);

        INDEX = (DocumentForwardIndex<Indexable>) Class.forName(indexClass).newInstance();
        INDEX.loadIndex(new Path(indexFile), new Path(mappingFile), fs);

        Server server = new Server(port);
        org.mortbay.jetty.servlet.Context root = new org.mortbay.jetty.servlet.Context(server, "/",
            org.mortbay.jetty.servlet.Context.SESSIONS);
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

        while (true);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  private DocumentForwardIndexHttpServer() {}

  // This must be public.
  public static class HomeServlet extends HttpServlet {

    static final long serialVersionUID = 8253865405L;
    static final Random r = new Random();

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException,
        IOException {
      res.setContentType("text/html");
      PrintWriter out = res.getWriter();

      out.println("<html><head><title>Collection Access: " + INDEX.getCollectionPath()
          + "</title><head>");
      out.println("<body>");

      out.println("<h3>Collection Access: " + INDEX.getCollectionPath() + "</h3>");

      int firstDocno = INDEX.getFirstDocno();
      int lastDocno = INDEX.getLastDocno();
      int numDocs = lastDocno - firstDocno;

      LOG.info("first docno: " + firstDocno);
      LOG.info("last docno: " + lastDocno);

      String firstDocid = INDEX.getDocid(firstDocno);
      String lastDocid = INDEX.getDocid(lastDocno);

      out.println("First document: docno <a href=\"/fetch_docno?docno=" + firstDocno + "\">"
          + firstDocno + "</a> or <a href=\"/fetch_docid?docid=" + firstDocid + "\">" + firstDocid
          + "</a><br/>");
      out.println("Last document: docno <a href=\"/fetch_docno?docno=" + lastDocno + "\">"
          + lastDocno + "</a> or <a href=\"/fetch_docid?docid=" + lastDocid + "\">" + lastDocid
          + "</a>");

      out.println("<h3>Fetch a docid</h3>");

      String id;

      out.println("<p>(random examples: ");

      id = INDEX.getDocid(r.nextInt(numDocs) + firstDocno);
      out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

      id = INDEX.getDocid(r.nextInt(numDocs) + firstDocno);
      out.println("<a href=\"/fetch_docid?docid=" + id + "\">" + id + "</a>, ");

      id = INDEX.getDocid(r.nextInt(numDocs) + firstDocno);
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

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
        IOException {
      LOG.info("triggered servlet for fetching document by docid");
      String docid = null;

      try {
        if (req.getParameterValues("docid") != null)
          docid = req.getParameterValues("docid")[0];

        Indexable doc = INDEX.getDocument(docid);

        if (doc != null) {
          LOG.info("fetched: " + doc.getDocid());
          res.setContentType(doc.getDisplayContentType());

          PrintWriter out = res.getWriter();
          out.print(doc.getDisplayContent());
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

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException,
        IOException {
      LOG.info("triggered servlet for fetching document by docno");

      int docno = 0;
      try {
        if (req.getParameterValues("docno") != null)
          docno = Integer.parseInt(req.getParameterValues("docno")[0]);

        Indexable doc = INDEX.getDocument(docno);

        if (doc != null) {
          LOG.info("fetched: " + doc.getDocid() + " = docno " + docno);
          res.setContentType(doc.getDisplayContentType());

          PrintWriter out = res.getWriter();
          out.print(doc.getDisplayContent());
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
  
  public static final String INDEX_OPTION = "index";
  public static final String MAPPING_OPTION = "docnoMapping";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) forward index path").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) DocnoMapping data path").create(MAPPING_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(MAPPING_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String indexFile = cmdline.getOptionValue(INDEX_OPTION);
    String mappingFile = cmdline.getOptionValue(MAPPING_OPTION);

    LOG.info("Launching DocumentForwardIndexHttpServer");
    LOG.info(" - index file: " + indexFile);
    LOG.info(" - docno mapping data file: " + mappingFile);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    Random rand = new Random();
    int r = rand.nextInt();

    // This tmp file as a rendezvous point.
    Path tmpPath = new Path("/tmp/" + r);

    if (fs.exists(tmpPath)) {
      fs.delete(tmpPath, true);
    }

    Job job = new Job(conf, DocumentForwardIndexHttpServer.class.getSimpleName());
    job.setJarByClass(DocumentForwardIndexHttpServer.class);

    job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
    job.getConfiguration().set(INDEX_KEY, indexFile);
    job.getConfiguration().set(DOCNO_MAPPING_KEY, mappingFile);
    job.getConfiguration().set(TMP_KEY, tmpPath.toString());

    job.setNumReduceTasks(0);
    job.setInputFormatClass(NullInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(MyMapper.class);

    job.submit();

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

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + DocumentForwardIndexHttpServer.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new DocumentForwardIndexHttpServer(), args);
  }
}