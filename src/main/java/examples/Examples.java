package examples;

import java.sql.*;

public class Examples {
  public static void main( String[] args ) throws SQLException, ClassNotFoundException {
    Class.forName("io.tiledb.TileDBCloudDriver");
//        Properties properties  = new Properties();
//        properties.setProperty("apiKey", "KEY");
//        properties.setProperty("rememberMe", "true");
    try (
            Connection conn = DriverManager.getConnection("jdbc:tiledb-cloud:<NAMESPACE>");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM `tiledb://TileDB-Inc/quickstart_sparse`");
    ){
      while (rs.next()) System.out.println(rs.getInt("rows") + " - " +
              rs.getInt("cols") + " - " +
              rs.getInt("a"));
    }
  }
}
