/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.tiledb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

public class JDBCTest {
  String token;
  Properties properties;

  @Before
  public void setup() {
    token = System.getenv("API_TOKEN");
    properties = new Properties();
    properties.setProperty("apiKey", token);
  }

  @Test
  public void dataTest() throws ClassNotFoundException, SQLException {
    Class.forName("io.tiledb.TileDBCloudDriver");

    String result = "";
    Connection conn = DriverManager.getConnection("jdbc:tiledb-cloud:TileDB-Inc", properties);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM `tiledb://TileDB-Inc/quickstart_sparse`");

    while (rs.next())
      result += rs.getInt("rows") + " - " + rs.getInt("cols") + " - " + rs.getInt("a") + "\n";

    String expectedOutput = "1 - 1 - 1\n2 - 3 - 3\n2 - 4 - 2\n";
    assertEquals(expectedOutput, result);

    // clean up
    conn.close();
    stmt.close();
    rs.close();
  }

  @Test
  public void metadataTest() throws ClassNotFoundException, SQLException {
    Class.forName("io.tiledb.TileDBCloudDriver");

    Connection conn = DriverManager.getConnection("jdbc:tiledb-cloud:TileDB-Inc", properties);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM `tiledb://TileDB-Inc/quickstart_sparse`");
    ResultSetMetaData metadata = rs.getMetaData();
    assertEquals(3, metadata.getColumnCount());

    assertEquals(Types.INTEGER, metadata.getColumnType(1));
    assertEquals(Types.INTEGER, metadata.getColumnType(2));
    assertEquals(Types.BIGINT, metadata.getColumnType(3));

    assertEquals("rows", metadata.getColumnName(1));
    assertEquals("cols", metadata.getColumnName(2));
    assertEquals("a", metadata.getColumnName(3));

    // clean up
    conn.close();
    stmt.close();
    rs.close();
  }
}
