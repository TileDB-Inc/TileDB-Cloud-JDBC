package io.tiledb;

import io.tiledb.cloud.Pair;
import io.tiledb.cloud.TileDBClient;
import io.tiledb.cloud.TileDBSQL;
import io.tiledb.cloud.rest_api.api.SqlApi;
import io.tiledb.cloud.rest_api.model.ResultFormat;
import io.tiledb.cloud.rest_api.model.SQLParameters;
import io.tiledb.util.Util;
import java.sql.*;
import java.util.ArrayList;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class TileDBCloudStatement implements Statement {
  private SqlApi sqlApi;
  private TileDBClient tileDBClient;

  private String namespace;

  private ResultSet resultSet;

  private ArrayList<VectorSchemaRoot> readBatches;

  /**
   * Constructor
   *
   * @param tileDBClient The TileDB client object
   * @param namespace The TileDB namespace
   */
  TileDBCloudStatement(TileDBClient tileDBClient, String namespace) {
    this.sqlApi = new SqlApi(tileDBClient.getApiClient());
    this.namespace = namespace;
    this.tileDBClient = tileDBClient;
    this.readBatches = new ArrayList<>();
  }

  @Override
  public ResultSet executeQuery(String s) throws SQLException {
    // create SQL parameters
    SQLParameters sqlParameters = new SQLParameters();
    String query = Util.useTileDBUris(s);
    sqlParameters.setQuery(query);
    // get results in arrow format
    sqlParameters.setResultFormat(ResultFormat.ARROW);

    // set timeout to unlimited
    tileDBClient.setReadTimeout(0);

    // create TileDBSQL object
    TileDBSQL tileDBSQL = new TileDBSQL(tileDBClient, namespace, sqlParameters);

    // run query and expect results in arrow format
    Pair<ArrayList<ValueVector>, Integer> valueVectors = tileDBSQL.execArrow();

    return new TileDBCloudResultSet(valueVectors, namespace);
  }

  @Override
  public int executeUpdate(String s) throws SQLException {
    return 0;
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int i) throws SQLException {}

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int i) throws SQLException {}

  @Override
  public void setEscapeProcessing(boolean b) throws SQLException {}

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int i) throws SQLException {}

  @Override
  public void cancel() throws SQLException {}

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public void setCursorName(String s) throws SQLException {}

  @Override
  public boolean execute(String s) throws SQLException {
    this.resultSet = this.executeQuery(s);
    return true;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int i) throws SQLException {}

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String s) throws SQLException {}

  @Override
  public void clearBatch() throws SQLException {}

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int i) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String s, int i) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    return true;
  }

  @Override
  public boolean execute(String s, String[] strings) throws SQLException {
    return true;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean b) throws SQLException {}

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {}

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }
}
