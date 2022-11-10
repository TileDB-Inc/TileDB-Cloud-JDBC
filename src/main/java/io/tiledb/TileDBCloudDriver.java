package io.tiledb;

import io.tiledb.cloud.TileDBLogin;
import io.tiledb.util.Util;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class TileDBCloudDriver implements Driver {
  private static final Driver INSTANCE = new TileDBCloudDriver();
  private static boolean registered;

  public TileDBCloudDriver() {}

  @Override
  public Connection connect(String s, Properties properties) throws SQLException {
    if (!acceptsURL(s)) return null;

    String[] parts = s.split(":");

    // get namespace from URL
    String namespace = parts[2];

    // call the appropriate connector depending on the properties given
    if (properties.isEmpty()) return new TileDBCloudConnection(namespace);
    return new TileDBCloudConnection(namespace, createLoginObject(properties));
  }

  /**
   * Create a TileDB login object based on the JDBC properties given as input.
   *
   * @param properties The properties
   * @return The login object
   */
  private TileDBLogin createLoginObject(Properties properties) {
    try {
      return new TileDBLogin(
          (String) properties.getOrDefault("username", null),
          (String) properties.getOrDefault("password", null),
          (String) properties.getOrDefault("apiKey", null),
          Boolean.parseBoolean((String) properties.getOrDefault("verifySSL", "true")),
          Boolean.parseBoolean((String) properties.getOrDefault("rememberMe", "false")),
          Boolean.parseBoolean((String) properties.getOrDefault("overwritePrevious", "false")));
    } catch (Exception e) {
      throw new RuntimeException("Error with the input properties");
    }
  }

  @Override
  public boolean acceptsURL(String s) throws SQLException {
    // check for URL correctness
    String[] parts = s.split(":");

    return parts.length >= 2
        && parts[0].equalsIgnoreCase("jdbc")
        && parts[1].equalsIgnoreCase("tiledb-cloud");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    DriverPropertyInfo[] driverPropertyInfo = new DriverPropertyInfo[5];
    driverPropertyInfo[0] =
        new DriverPropertyInfo("Username", (String) properties.getOrDefault("username", null));
    driverPropertyInfo[0].required = false;
    driverPropertyInfo[0].description = "TileDB username";
    driverPropertyInfo[0].choices = new String[] {"Dimitris", "Stavros", "Seth"};

    driverPropertyInfo[1] =
        new DriverPropertyInfo("Password", (String) properties.getOrDefault("password", null));
    driverPropertyInfo[1].required = false;
    driverPropertyInfo[1].description = "TileDB password";
    driverPropertyInfo[1].choices = new String[] {"******"};

    driverPropertyInfo[2] =
        new DriverPropertyInfo("API token", (String) properties.getOrDefault("apiKey", null));
    driverPropertyInfo[2].required = false;
    driverPropertyInfo[2].description =
        "The TileDB API token which can be found/created in the TileDB console";
    driverPropertyInfo[2].choices =
        new String[] {
          "eyJhbGciOikdjsnciiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiODFkYTFhN2EtYzQyOC00ZDU5LTliYzEtOTVmZmZkZTUzMzI4IiwiU2VlZCI6ODM5MTc1Njc0ODU5MzMzNCwiZXhwIjoxNzEzNjQ2Nzk5LCJpYXQiOjE2NjY3MTIxMDcsIm5iZiI6MTYlpoirusEwNywic3ViIjoiZHN0YXJhIn0.Tz-OIVHWb3VNC8DR8tTiqnapcG1kntbZSawDnnJsFGc"
        };

    driverPropertyInfo[3] =
        new DriverPropertyInfo(
            "Remember me", (String) properties.getOrDefault("rememberMe", "false"));
    driverPropertyInfo[3].required = true;
    driverPropertyInfo[3].description =
        "Set to true to instruct TileDB to remember the login credentials provided";
    driverPropertyInfo[3].choices = new String[] {"true", "false"};

    driverPropertyInfo[4] =
        new DriverPropertyInfo("Verify SSL", (String) properties.getOrDefault("verifySSL", "true"));
    driverPropertyInfo[4].required = true;
    driverPropertyInfo[4].description = "Set to true to instruct TileDB to verify SSL";
    driverPropertyInfo[4].choices = new String[] {"true", "false"};

    driverPropertyInfo[5] =
        new DriverPropertyInfo(
            "Overwrite previous", (String) properties.getOrDefault("overwritePrevious", "false"));
    driverPropertyInfo[5].required = true;
    driverPropertyInfo[5].description =
        "Set to true to overwrite the previously saved credentials. Can be used in combination with -Remember me-";
    driverPropertyInfo[5].choices = new String[] {"true", "false"};

    return driverPropertyInfo;
  }

  @Override
  public int getMajorVersion() {
    return Util.VERSION_MAJOR;
  }

  @Override
  public int getMinorVersion() {
    return Util.VERSION_MINOR;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  public static synchronized Driver load() {
    if (!registered) {
      registered = true;
      try {
        DriverManager.registerDriver(INSTANCE);
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }
    }

    return INSTANCE;
  }

  static {
    load();
  }
}
