package io.tiledb.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {
  public static int VERSION_MINOR = 1;
  public static final int VERSION_REVISION = 3;
  public static int VERSION_MAJOR = 3;

  public static int TILEDB_CLOUD_VERSION_MINOR = 0;
  public static int TILEDB_CLOUD_VERSION_REVISION = 2;
  public static int TILEDB_CLOUD_VERSION_MAJOR = 0;

  public static String SCHEMA_NAME = "All TileDB arrays";

  /**
   * Replaces all array names with the corresponding UUID if the input string is of a specific
   * pattern
   *
   * @param completeURI If the input string is of the form "tiledb://TileDB-Inc/orders ~
   *     tiledb://TileDB-Inc/120f0518-dd8d-467f-8c1b-23aa49929465" this method will modify the input
   *     string to only use the TileDB URI with the UUID. If an input string does not match the
   *     regex pattern specified in the Pattern.compile method, the Matcher will not find any
   *     matches, and the code will simply return the original input string as it is without making
   *     any modifications.
   * @return
   */
  public static String replaceArrayNamesWithUUIDs(String completeURI) {
    // Define the regular expression pattern for the TileDB URI pattern with spaces around "~"
    String regex = "(tiledb://[^~]+) ~ ([^\\s]+)";
    Pattern pattern = Pattern.compile(regex);

    Matcher matcher = pattern.matcher(completeURI);
    StringBuilder modifiedText = new StringBuilder();

    // Find and remove the first part in TileDB URIs
    int lastEnd = 0;
    while (matcher.find()) {
      // Append the text before the match and a space
      modifiedText.append(completeURI.substring(lastEnd, matcher.start(1)));

      // Append the text after "~" and a space
      modifiedText.append(matcher.group(2));

      // Update the lastEnd to the end of this match
      lastEnd = matcher.end();
    }

    // Append any remaining text after the last match
    modifiedText.append(completeURI.substring(lastEnd));

    return modifiedText.toString().trim();
  }
}
