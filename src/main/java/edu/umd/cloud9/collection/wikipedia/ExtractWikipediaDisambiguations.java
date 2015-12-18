package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Tool for taking a Wikipedia XML dump file and writing out article titles and ambiguous related titles
 * in a flat text file (article title and related titles, separated by tabs; related titles are '\002' separated).
 *
 * @author Gaurav Ragtah (gaurav.ragtah@lithium.com)
 */
public class ExtractWikipediaDisambiguations extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractWikipediaDisambiguations.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
  }

  private static class WikiDisambiguationMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {

    private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
    private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");
    private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
    // Sometimes, URLs bump up against comments e.g., <!-- http://foo.com/-->
    // So remove comments first, since the URL pattern might capture comment terminators.
    private static final Pattern URL = Pattern.compile("http://[^ <]+");
    private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");
    private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>");
    private static final Pattern NEWLINE = Pattern.compile("[\\r\\n]+");
    private static final String SINGLE_SPACE = " ";

    private static final Pattern WIKI_TITLE = Pattern.compile("\\[\\[(.*?)\\]\\]");
    private static final Pattern WIKI_TITLE_DUPLICATED = Pattern.compile("\\|.+");

    private static final Pattern[] patternsToCleanUp = {LANG_LINKS, REF, HTML_COMMENT, URL, DOUBLE_CURLY, HTML_TAG, NEWLINE};

    @Override
    public void map(LongWritable key, WikipediaPage p, Context context) throws IOException, InterruptedException {
      context.getCounter(PageTypes.TOTAL).increment(1);

      if (p.isEmpty()) {
        context.getCounter(PageTypes.EMPTY).increment(1);
      } else if (p.isRedirect()) {
        context.getCounter(PageTypes.REDIRECT).increment(1);
      } else if (p.isDisambiguation()) {

        context.getCounter(PageTypes.DISAMBIGUATION).increment(1);

        ArrayList<String> wikiTitleList = new ArrayList<String>();
        Text title = new Text();
        Text similarTitles = new Text();

        String wikiText = p.getWikiMarkup();

        // Find 'See also' section and truncate it - related but not ambiguous terms
        int seeAlsoSectionStart = wikiText.indexOf("See also");
        if (seeAlsoSectionStart >= 0)
          wikiText = wikiText.substring(0,seeAlsoSectionStart);

        if (wikiText == null) {
          context.getCounter(WikiDisambiguationMapper.class.getSimpleName(), "NULL_WIKITEXT").increment(1);
          return;
        }

        // The way the some entities are encoded, we have to unescape twice.
        wikiText = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(wikiText));

        for (Pattern pattern: patternsToCleanUp) {
          wikiText = pattern.matcher(wikiText).replaceAll(SINGLE_SPACE);
        }

        Pattern disambPattern = p.getDisambPattern();
        String disambRegex = disambPattern.toString();

        // Format disambiguation regex to match against in-text article titles.
        // Eg. Convert \{disambig\w*\} to \(disambig\w*\)
        disambRegex = "\\(" + disambRegex.replaceAll("(\\\\\\{)?(\\\\\\})?", "") + "\\)";
        Pattern disambMatcher = Pattern.compile(disambRegex, Pattern.CASE_INSENSITIVE);

        // Extract the ambiguous entity from the disambiguation article title.
        // Eg. Apple_Store_(disambiguation) to Apple Store
        String ambiguousTitle = disambMatcher.matcher(p.getTitle()).replaceAll(SINGLE_SPACE).replaceAll("_", SINGLE_SPACE).toLowerCase().trim();

        Matcher wikiTitleMatcher = WIKI_TITLE.matcher(wikiText);
        while (wikiTitleMatcher.find()) {
          String wikiTitle = wikiTitleMatcher.group(1);

          // Only pick in-text titles that are not disambiguations and contain the current page's ambiguous title.
          // Eg. From the Apple disambiguation page, pick [[Apple Inc.]] and [[Big Apple]] but not [[Big Apple (disambiguation)]] or [[Apel (disambiguation)]]
          if (!disambMatcher.matcher(wikiTitle).find() && wikiTitle.toLowerCase().contains(ambiguousTitle)) {
            // To handle duplication like [[Alien (Britney Spears song)|"Alien" (Britney Spears song)]] in the same in-text title
            wikiTitle = WIKI_TITLE_DUPLICATED.matcher(wikiTitle).replaceAll(SINGLE_SPACE).trim();
            wikiTitleList.add(wikiTitle);
          }
        }

        String[] wikiTitles = wikiTitleList.toArray(new String[wikiTitleList.size()]);
        for (int i = 0; i < wikiTitles.length; i++) {
          title.clear();
          similarTitles.clear();
          title.set(wikiTitles[i]);
          String similarTitlesStr = "";

          for (int j = 0; j < wikiTitles.length; j++) {
            if (i == j || wikiTitles[j].isEmpty()) continue;
            similarTitlesStr += wikiTitles[j];
            if (j < wikiTitles.length - 1) similarTitlesStr += '\002';
          }

          if (!similarTitlesStr.isEmpty()) {
            similarTitles.set(similarTitlesStr);
            context.write(title, similarTitles);
          }
        }

      } else if (p.isArticle()) {
        context.getCounter(PageTypes.ARTICLE).increment(1);
        if (p.isStub()) {
          context.getCounter(PageTypes.STUB).increment(1);
        }
      } else {
        context.getCounter(PageTypes.OTHER).increment(1);
      }
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|nl|de|fr|ru|it|es|vi|pl|ja|pt|zh|uk|ca|fa|no|fi|id|ar|sr|ko|hi|zh_yue|cs|tr").hasArg()
        .withDescription("two-letter or six-letter language code").create(LANGUAGE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      LOG.error("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String language = "en"; // Assume "en" by default.
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if (!(language.length() == 2 || language.length() == 6)) {
        LOG.error("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - language: " + language);

    Job job = Job.getInstance(getConf());
    job.setJarByClass(ExtractWikipediaDisambiguations.class);
    job.setJobName(String.format("ExtractWikipediaDisambiguations[%s: %s, %s: %s, %s: %s]", INPUT_OPTION,
        inputPath, OUTPUT_OPTION, outputPath, LANGUAGE_OPTION, language));

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WikiDisambiguationMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public ExtractWikipediaDisambiguations() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractWikipediaDisambiguations(), args);
  }
}
