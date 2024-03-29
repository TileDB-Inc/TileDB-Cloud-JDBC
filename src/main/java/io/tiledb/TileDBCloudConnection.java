package io.tiledb;

import io.tiledb.cloud.TileDBClient;
import io.tiledb.cloud.TileDBLogin;
import io.tiledb.cloud.rest_api.api.ArrayApi;
import io.tiledb.cloud.rest_api.model.ArrayBrowserData;
import io.tiledb.cloud.rest_api.model.FileType;
import io.tiledb.util.Util;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TileDBCloudConnection implements java.sql.Connection {
  private ArrayApi arrayApi;
  private TileDBClient tileDBClient;
  private String namespace;
  private boolean listPublicArrays;

  Logger logger = Logger.getLogger(TileDBCloudConnection.class.getName());

  /** @param namespace The TileDB namespace */
  TileDBCloudConnection(String namespace, boolean listPublicArrays) {
    this.tileDBClient = new TileDBClient();
    this.arrayApi = new ArrayApi(tileDBClient.getApiClient());
    this.namespace = namespace;
    this.listPublicArrays = listPublicArrays;
  }

  /**
   * @param namespace The TileDB namespace
   * @param login The TileDB login object
   */
  TileDBCloudConnection(String namespace, TileDBLogin login, boolean listPublicArrays) {
    this.tileDBClient = new TileDBClient(login);
    this.arrayApi = new ArrayApi(tileDBClient.getApiClient());
    this.namespace = namespace;
    this.listPublicArrays = listPublicArrays;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new TileDBCloudStatement(tileDBClient, namespace);
  }

  @Override
  public PreparedStatement prepareStatement(String s) throws SQLException {
    // make sql compatible with external apps
    s = makeTableauCompatible(s);
    s = makePowerBICompatible(s);

    Statement statement = this.createStatement();
    ResultSet resultSet = statement.executeQuery(s);

    return new TileDBCloudPrepareStatement(resultSet);
  }

  /**
   * Making the driver fully compatible with Power BI's sql syntax.*
   *
   * @param sql The input query
   * @return The TIleDB compatible query
   */
  private String makePowerBICompatible(String sql) {
    String regex = "`" + Util.SCHEMA_NAME + "`.";
    return sql.replaceAll("\"", "`").replaceAll(regex, "");
  }

  /**
   * Tableau adds some custom sql in the queries. This method removes it.
   *
   * @param s The input query
   * @return The TileDB compatible query.
   */
  private String makeTableauCompatible(String s) {
    if (!s.contains("Custom SQL Query"))
      return s; // if it does not contain this string it is no Tableau

    int first = s.indexOf("(");
    s = s.substring(first + 1);

    int last = s.indexOf("Custom");
    s = s.substring(0, last - 3);

    logger.log(Level.INFO, "Query is: " + s);
    return s;
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    return null;
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    return null;
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {}

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {}

  @Override
  public void rollback() throws SQLException {}

  @Override
  public void close() throws SQLException {}

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    // load data here instead of inside the metadata class to avoid multiple loads
    TileDBCloudDatabaseMetadata tileDBCloudDatabaseMetadata = new TileDBCloudDatabaseMetadata();
    tileDBCloudDatabaseMetadata.setNamespace(this.namespace);
    tileDBCloudDatabaseMetadata.setArrayApi(arrayApi);
    List<String> excludeFileType =
        Arrays.asList(
            FileType.NOTEBOOK.toString(),
            FileType.FILE.toString(),
            FileType.ML_MODEL.toString(),
            FileType.REGISTERED_TASK_GRAPH.toString(),
            FileType.USER_DEFINED_FUNCTION.toString());

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Future<?>> tasks = new ArrayList<>();

    // owned arrays
    tasks.add(
        executorService.submit(
            () -> {
              try {
                ArrayBrowserData resultOwned =
                    arrayApi.arraysBrowserOwnedGet(
                        null,
                        null,
                        null,
                        namespace,
                        null,
                        null,
                        null,
                        null,
                        null,
                        excludeFileType,
                        null);
                tileDBCloudDatabaseMetadata.setArraysOwned(resultOwned);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }));

    // shared arrays
    tasks.add(
        executorService.submit(
            () -> {
              try {
                List<String> sharedTo = Arrays.asList(namespace);
                ArrayBrowserData resultShared =
                    arrayApi.arraysBrowserSharedGet(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        excludeFileType,
                        null,
                        sharedTo);
                tileDBCloudDatabaseMetadata.setArraysShared(resultShared);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }));

    // public arrays
    if (listPublicArrays) {
      tasks.add(
          executorService.submit(
              () -> {
                try {
                  ArrayBrowserData resultPublic =
                      arrayApi.arraysBrowserPublicGet(
                          null,
                          null,
                          null,
                          null,
                          "name",
                          null,
                          null,
                          null,
                          null,
                          excludeFileType,
                          null);
                  tileDBCloudDatabaseMetadata.setArraysPublic(resultPublic);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }));
    }

    // Shutdown the executor and wait for all threads to complete
    executorService.shutdown();
    for (Future<?> future : tasks) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    return tileDBCloudDatabaseMetadata;
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {}

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String s) throws SQLException {}

  @Override
  public String getCatalog() throws SQLException {
    return "TileDB-Catalog";
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {}

  @Override
  public int getTransactionIsolation() throws SQLException {

    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {

    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public Statement createStatement(int i, int i1) throws SQLException {

    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {

    return null;
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {

    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {

    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

  @Override
  public void setHoldability(int i) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {

    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {

    return null;
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {}

  @Override
  public Statement createStatement(int i, int i1, int i2) throws SQLException {

    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {

    return null;
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {

    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {

    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {

    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {

    return null;
  }

  @Override
  public Clob createClob() throws SQLException {

    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {

    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {

    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {

    return null;
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    return false;
  }

  @Override
  public void setClientInfo(String s, String s1) throws SQLClientInfoException {}

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

  @Override
  public String getClientInfo(String s) throws SQLException {

    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {

    return null;
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {

    return null;
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {

    return null;
  }

  @Override
  public void setSchema(String s) throws SQLException {}

  @Override
  public String getSchema() throws SQLException {

    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {}

  @Override
  public void setNetworkTimeout(Executor executor, int i) throws SQLException {}

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
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
