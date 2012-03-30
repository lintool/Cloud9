package edu.umd.cloud9.webgraph;

public class DriverUtil {
  // raw link information is stored at /base/path/extracted.links
  public static final String OUTPUT_EXTRACT_LINKS = "extracted.links";

  // reverse web graph w/ lines of anchor text is stored at
  // /base/path/reverseWebGraph
  public static final String OUTPUT_REVERSE_WEBGRAPH = "reverseWebGraph";

  // web graph is stored at /base/path/webGraph
  public static final String OUTPUT_WEBGRAPH = "webGraph";

  // hostname information (for computing default weights) is stored at
  // /base/path/hostnames
  public static final String OUTPUT_HOST_NAMES = "hostnames";

  // reverse web graph w/ weighted lines of anchor text is stored at
  // /base/path/weightedReverseWebGraph
  public static final String OUTPUT_WEGIHTED_REVERSE_WEBGRAPH = "weightedReverseWebGraph";

  /**
   * Default number of reducers
   */
  public static final int DEFAULT_REDUCERS = 200;

  public static final String CL_COLLECTION = "-collection";
  public static final String CL_INPUT_FORMAT = "-inputFormat";
  public static final String CL_DOCNO_MAPPING_CLASS = "-docnoClass";
  public static final String CL_INCLUDE_INTERNAL_LINKS = "-il";
  public static final String CL_COMPUTE_WEIGHTS = "-caw";
  public static final String CL_NORMALIZER = "-normalizer";
  public static final String CL_BEGIN_SEGMENT = "-begin";
  public static final String CL_END_SEGMENT = "-end";
  public static final String CL_INPUT = "-input";
  public static final String CL_OUTPUT = "-output";
  public static final String CL_DOCNO_MAPPING = "-docno";
  public static final String CL_MAX_LENGTH = "-maxLength";
  public static final String CL_NUMBER_OF_REDUCERS = "-numReducers";

  public static String argValue(String[] args, String option) throws IllegalArgumentException {
    for(int i = 0; i < args.length - 1; i++) {
      if(args[i].equals(option)) {
        return args[i + 1];
      }
    }
    throw new IllegalArgumentException("Option not found: " + option );
  }

  public static boolean argExists(String[] args, String option) {
    for(int i = 0; i < args.length; i++) {
      if(args[i].equals(option)) {
        return true;
      }
    }
    return false;
  }
}
