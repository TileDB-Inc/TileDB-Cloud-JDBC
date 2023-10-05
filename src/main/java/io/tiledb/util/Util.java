package io.tiledb.util;

import io.tiledb.TileDBCloudTablesResultSet;
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
   * Takes as input a complete query that might or might not contain references to arrays of the
   * form [tiledb://../..][a09uhugr] and returns a query in which all these references which are
   * specific to this JDBC driver are translated to queries that contain the tiledbURis. This method
   * is mainly used when the query is created by a BI tool rather than a pure Java user. BI tools
   * display the arrays and when selected use their names to query them. Since the array names are
   * not unique in TileDB-Cloud we display the names accompanied with one small part of the original
   * UUID. We then map this UUID to the tileDBURI and modify the query so that it only uses
   * tileDBURIs. If no array references of the above-mentioned form are found the query is returned
   * intact.
   *
   * @param query The complete query
   * @return
   */
  public static String useTileDBUris(String query) {
    // Regular expression pattern to match the specified format
    Pattern pattern = Pattern.compile("\\[tiledb://([^/]+)/([^\\]]+)\\]\\[(\\w+)\\]");

    // Matcher to find and replace occurrences of the pattern in the input string
    Matcher matcher = pattern.matcher(query);

    // StringBuffer to hold the modified input string
    StringBuffer result = new StringBuffer();

    // Find and replace occurrences of the pattern in the input string
    while (matcher.find()) {
      String key = matcher.group(3);

      String replacement =
          TileDBCloudTablesResultSet.shortUUIDToUri.getOrDefault(
              key, ""); // Get replacement from the map
      if (!replacement.equals(""))
        matcher.appendReplacement(
            result, replacement); // Replace the match with the corresponding value
    }
    matcher.appendTail(result); // Append the remaining part of the input string
    // Output the modified string
    return result.toString();
  }

  /**
   * Takes as input a TileDBURI and returns the first 8 characters of the UUID.
   *
   * @param tiledbUri the TileDBURI
   * @return the first 8 chars of the UUID
   */
  public static String getUUIDStart(String tiledbUri) {
    // Split the input URI by '/'
    String[] uriParts = tiledbUri.split("/");

    // Extract the desired string and return its first 8 characters
    String targetString = uriParts[3];
    return targetString.substring(0, Math.min(targetString.length(), 8));
  }

  public static String removeUUID(String arrayName) {
    // Regular expression pattern to match the specified format
    Pattern pattern = Pattern.compile("\\[tiledb://([^\\]]+)\\].*");

    // Matcher to find the pattern in the input string
    Matcher matcher = pattern.matcher(arrayName);

    // Check if the pattern is found and extract the desired part
    if (matcher.matches()) {
      return "tiledb://" + matcher.group(1);
    } else {
      return arrayName;
    }
  }
}
