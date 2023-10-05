package io.tiledb;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.tiledb.cloud.rest_api.model.ArrayBrowserData;
import io.tiledb.cloud.rest_api.model.ArrayInfo;
import io.tiledb.util.Util;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Logger;

public class TileDBCloudTablesResultSet implements ResultSet {
  public static BiMap<String, String> shortUUIDToUri = HashBiMap.create();
  private List<ArrayInfo> arraysOwned = new ArrayList<ArrayInfo>();
  private List<ArrayInfo> arraysShared = new ArrayList<ArrayInfo>();
  private List<ArrayInfo> arraysPublic = new ArrayList<ArrayInfo>();
  private int arrayIndex;
  private ArrayInfo currentArray;
  private int numberOfArrays;

  private Logger logger = Logger.getLogger(TileDBCloudTablesResultSet.class.getName());

  public TileDBCloudTablesResultSet(
      ArrayBrowserData arraysOwnedData,
      ArrayBrowserData arraysSharedData,
      ArrayBrowserData arraysPublicData) {

    if (arraysOwnedData != null) this.arraysOwned = arraysOwnedData.getArrays();

    if (arraysSharedData != null) this.arraysShared = arraysSharedData.getArrays();

    if (arraysPublicData != null) this.arraysPublic = arraysPublicData.getArrays();

    this.arrayIndex = -1;

    this.numberOfArrays =
        this.arraysOwned.size() + this.arraysShared.size() + this.arraysPublic.size();
    populateURIs();
  }

  private void populateURIs() {

    // Iterate through arraysOwned and add entries to the HashMap
    for (ArrayInfo arrayInfo : arraysOwned) {
      String key = Util.getUUIDStart(arrayInfo.getTiledbUri());
      String value = arrayInfo.getTiledbUri();
      putURI(key, value);
    }

    // Iterate through arraysShared and add entries to the HashMap
    for (ArrayInfo arrayInfo : arraysShared) {
      String key = Util.getUUIDStart(arrayInfo.getTiledbUri());
      String value = arrayInfo.getTiledbUri();
      putURI(key, value);
    }

    // Iterate through arraysPublic and add entries to the HashMap
    for (ArrayInfo arrayInfo : arraysPublic) {
      String key = Util.getUUIDStart(arrayInfo.getTiledbUri());
      String value = arrayInfo.getTiledbUri();
      putURI(key, value);
    }
  }

  /**
   * Puts a short UUID and value to the hashmap. If the key exists it will append "-1" or "-2", etc.
   *
   * @param key
   * @param value
   */
  private void putURI(String key, String value) {
    int counter = 1;
    while (shortUUIDToUri.containsKey(key)) {
      key = key + "-" + counter;
      counter++;
    }
    // we might have an array that is both public and shared. In such cases there is
    // no need to display the array twice
    if (!shortUUIDToUri.inverse().containsKey(value)) shortUUIDToUri.put(key, value);
  }

  public TileDBCloudTablesResultSet() {
    this.numberOfArrays = 0;
  }

  @Override
  public boolean next() throws SQLException {
    arrayIndex++;
    // Check arraysOwned
    if (arrayIndex < arraysOwned.size()) {
      currentArray = arraysOwned.get(arrayIndex);
      return true;
    }

    // Check arraysShared
    if (arrayIndex - arraysOwned.size() < arraysShared.size()) {
      currentArray = arraysShared.get(arrayIndex - arraysOwned.size());
      return true;
    }

    // Check arraysPublic
    if (arrayIndex - arraysOwned.size() - arraysShared.size() < arraysPublic.size()) {
      currentArray = arraysPublic.get(arrayIndex - arraysOwned.size() - arraysShared.size());
      return true;
    }

    // No more arrays to iterate
    return false;
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return "[tiledb://"
        + currentArray.getNamespace()
        + "/"
        + currentArray.getName()
        + "]["
        + shortUUIDToUri.inverse().get(currentArray.getTiledbUri())
        + "]";
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {

    return false;
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {

    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {

    return null;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {

    return new byte[0];
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {

    return null;
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {

    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {

    return null;
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    String ownership = "Owned";
    if (arrayIndex > this.arraysOwned.size() - 1) ownership = "Shared";
    if (arrayIndex > this.arraysShared.size() - 1) ownership = "Public";

    switch (columnLabel) {
      case "TABLE_NAME":
        return "[tiledb://"
            + currentArray.getNamespace()
            + "/"
            + currentArray.getName()
            + "]["
            + shortUUIDToUri.inverse().get(currentArray.getTiledbUri())
            + "]";
      case "REMARKS":
        return ownership + " TileDB URI: " + currentArray.getTiledbUri();
      case "TABLE_TYPE":
        return "TABLE";
      case "TABLE_SCHEM":
        return Util.SCHEMA_NAME;
      case "TABLE_CATALOG":
        return currentArray.getNamespace();
    }
    return "";
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {

    return false;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {

    return null;
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new TileDBCloudTablesResultSetMetadata(
        this.arraysOwned, this.arraysShared, this.arraysPublic, this.numberOfArrays);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {

    return 0;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {}

  @Override
  public void afterLast() throws SQLException {}

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    return arrayIndex + 1;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {}

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {}

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {}

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {}

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {}

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {}

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {}

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {}

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {}

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {}

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {}

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {}

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {}

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {}

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {}

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {}

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {}

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {}

  @Override
  public void updateNull(String columnLabel) throws SQLException {}

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {}

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {}

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {}

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {}

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {}

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {}

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {}

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {}

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {}

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {}

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {}

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {}

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {}

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {}

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {}

  @Override
  public void insertRow() throws SQLException {}

  @Override
  public void updateRow() throws SQLException {}

  @Override
  public void deleteRow() throws SQLException {}

  @Override
  public void refreshRow() throws SQLException {}

  @Override
  public void cancelRowUpdates() throws SQLException {}

  @Override
  public void moveToInsertRow() throws SQLException {}

  @Override
  public void moveToCurrentRow() throws SQLException {}

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {}

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {}

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {}

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {}

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {}

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {}

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {}

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {}

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {}

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {}

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {}

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {}

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {}

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {}

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {}

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {}

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {}

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {}

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {}

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {}

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {}

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
