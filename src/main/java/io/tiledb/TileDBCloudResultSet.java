package io.tiledb;

import io.tiledb.java.api.Pair;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Logger;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.Text;

public class TileDBCloudResultSet implements ResultSet {

  private Logger logger = Logger.getLogger(TileDBCloudResultSet.class.getName());
  private int readBatchCount;
  private ArrayList<ValueVector> valueVectors;
  private int currentRow;
  private int currentBatch;
  private int fieldsPerBatch = 0;
  private int globalRowCount;
  private String namespace;
  private Map<String, Integer> columnToPosition;

  public TileDBCloudResultSet(
      Pair<ArrayList<ValueVector>, Integer> resultsArrow, String namespace) {
    this.readBatchCount = resultsArrow.getSecond();
    this.valueVectors = resultsArrow.getFirst();
    this.currentRow = -1;
    this.currentBatch = 0;
    this.globalRowCount = 1;
    this.namespace = namespace;
    if (readBatchCount != 0) this.fieldsPerBatch = this.valueVectors.size() / readBatchCount;
    this.columnToPosition = new HashMap<>();

    buildColumnsToPosition();
  }

  /** Build hashMap with index and column names */
  private void buildColumnsToPosition() {
    columnToPosition.clear();

    for (int i = 0; i < this.fieldsPerBatch; i++) {
      columnToPosition.put(valueVectors.get(i).getName(), i + 1);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (valueVectors.get(currentBatch).getValueCount() - 1 > currentRow) {
      currentRow++;
      globalRowCount++;
    } else {
      currentBatch++;
      currentRow = 0;
    }
    return currentBatch < readBatchCount;
  }

  @Override
  public void close() throws SQLException {}

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(int i) throws SQLException {
    Text text =
        (Text) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
    return text.toString();
  }

  @Override
  public boolean getBoolean(int i) throws SQLException {
    int flag =
        (int) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
    return flag == 1;
  }

  @Override
  public byte getByte(int i) throws SQLException {
    return (byte) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public short getShort(int i) throws SQLException {
    return (short) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public int getInt(int i) throws SQLException {
    return (int) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public long getLong(int i) throws SQLException {
    return (long) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public float getFloat(int i) throws SQLException {
    return (float) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public double getDouble(int i) throws SQLException {
    return (double) valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
    return (BigDecimal)
        valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public byte[] getBytes(int i) throws SQLException {
    Object obj = valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    try (ObjectOutputStream ois = new ObjectOutputStream(boas)) {
      ois.writeObject(obj);
      return boas.toByteArray();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    throw new RuntimeException();
  }

  @Override
  public Date getDate(int i) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int i) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    return null;
  }

  @Override
  public String getString(String s) throws SQLException {
    return this.getString(findColumn(s));
  }

  @Override
  public boolean getBoolean(String s) throws SQLException {
    return this.getBoolean(findColumn(s));
  }

  @Override
  public byte getByte(String s) throws SQLException {
    return this.getByte(findColumn(s));
  }

  @Override
  public short getShort(String s) throws SQLException {
    return this.getShort(findColumn(s));
  }

  @Override
  public int getInt(String s) throws SQLException {
    return this.getInt(findColumn(s));
  }

  @Override
  public long getLong(String s) throws SQLException {
    return this.getLong(findColumn(s));
  }

  @Override
  public float getFloat(String s) throws SQLException {
    return this.getFloat(findColumn(s));
  }

  @Override
  public double getDouble(String s) throws SQLException {
    return this.getDouble(findColumn(s));
  }

  @Override
  public BigDecimal getBigDecimal(String s, int i) throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(String s) throws SQLException {
    return this.getBytes(findColumn(s));
  }

  @Override
  public Date getDate(String s) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String s) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
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
    TileDBCloudResultSetMetadata tileDBCloudResultSetMetadata = new TileDBCloudResultSetMetadata();
    tileDBCloudResultSetMetadata.setColumnCount(fieldsPerBatch);
    tileDBCloudResultSetMetadata.setValueVectors(valueVectors);
    tileDBCloudResultSetMetadata.setNamespace(namespace);
    return tileDBCloudResultSetMetadata;
  }

  @Override
  public Object getObject(int i) throws SQLException {
    return valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public Object getObject(String s) throws SQLException {
    return this.getObject(findColumn(s));
  }

  @Override
  public int findColumn(String s) throws SQLException {
    return this.columnToPosition.get(s);
  }

  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    return (BigDecimal)
        valueVectors.get(i - 1 + (currentBatch * fieldsPerBatch)).getObject(currentRow);
  }

  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    return this.getBigDecimal(findColumn(s));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return currentRow == -1;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return (currentBatch == readBatchCount - 1)
        && currentRow > valueVectors.get(currentBatch * fieldsPerBatch).getValueCount() - 1;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return currentRow == 0 && currentBatch == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    return (currentBatch == readBatchCount - 1)
        && currentRow == valueVectors.get(currentBatch * fieldsPerBatch).getValueCount() - 1;
  }

  @Override
  public void beforeFirst() throws SQLException {
    this.currentRow = -1;
    this.currentBatch = 0;
  }

  @Override
  public void afterLast() throws SQLException {
    currentBatch = readBatchCount - 1;
    currentRow = valueVectors.get(currentBatch * fieldsPerBatch).getValueCount();
  }

  @Override
  public boolean first() throws SQLException {
    this.currentRow = -1;
    this.currentBatch = 0;
    return valueVectors.get(0).getValueCount() > 0;
  }

  @Override
  public boolean last() throws SQLException {
    currentBatch = readBatchCount - 1;
    currentRow = valueVectors.get(currentBatch * fieldsPerBatch).getValueCount() - 1;
    return valueVectors.get(0).getValueCount() > 0;
  }

  @Override
  public int getRow() throws SQLException {
    return globalRowCount;
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
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
  public int getType() throws SQLException {
    return 0;
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
  public void updateNull(int i) throws SQLException {}

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {}

  @Override
  public void updateByte(int i, byte b) throws SQLException {}

  @Override
  public void updateShort(int i, short i1) throws SQLException {}

  @Override
  public void updateInt(int i, int i1) throws SQLException {}

  @Override
  public void updateLong(int i, long l) throws SQLException {}

  @Override
  public void updateFloat(int i, float v) throws SQLException {}

  @Override
  public void updateDouble(int i, double v) throws SQLException {}

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {}

  @Override
  public void updateString(int i, String s) throws SQLException {}

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {}

  @Override
  public void updateDate(int i, Date date) throws SQLException {}

  @Override
  public void updateTime(int i, Time time) throws SQLException {}

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {}

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {}

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {}

  @Override
  public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {}

  @Override
  public void updateObject(int i, Object o, int i1) throws SQLException {}

  @Override
  public void updateObject(int i, Object o) throws SQLException {}

  @Override
  public void updateNull(String s) throws SQLException {}

  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {}

  @Override
  public void updateByte(String s, byte b) throws SQLException {}

  @Override
  public void updateShort(String s, short i) throws SQLException {}

  @Override
  public void updateInt(String s, int i) throws SQLException {}

  @Override
  public void updateLong(String s, long l) throws SQLException {}

  @Override
  public void updateFloat(String s, float v) throws SQLException {}

  @Override
  public void updateDouble(String s, double v) throws SQLException {}

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {}

  @Override
  public void updateString(String s, String s1) throws SQLException {}

  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {}

  @Override
  public void updateDate(String s, Date date) throws SQLException {}

  @Override
  public void updateTime(String s, Time time) throws SQLException {}

  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {}

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {}

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {}

  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {}

  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {}

  @Override
  public void updateObject(String s, Object o) throws SQLException {}

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
  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int i) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String s) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int i) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String s) throws SQLException {

    return null;
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {}

  @Override
  public void updateRef(String s, Ref ref) throws SQLException {}

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {}

  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {}

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {}

  @Override
  public void updateClob(String s, Clob clob) throws SQLException {}

  @Override
  public void updateArray(int i, Array array) throws SQLException {}

  @Override
  public void updateArray(String s, Array array) throws SQLException {}

  @Override
  public RowId getRowId(int i) throws SQLException {

    return null;
  }

  @Override
  public RowId getRowId(String s) throws SQLException {

    return null;
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {}

  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int i, String s) throws SQLException {}

  @Override
  public void updateNString(String s, String s1) throws SQLException {}

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {}

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {}

  @Override
  public NClob getNClob(int i) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {}

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {}

  @Override
  public String getNString(int i) throws SQLException {

    return null;
  }

  @Override
  public String getNString(String s) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {}

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {}

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {}

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {}

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {}

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {}

  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {}

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {}

  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {}

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {}

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {}

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {}

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {}

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {}

  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {}

  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {}

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {}

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {}

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {}

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {}

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {}

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {}

  @Override
  public <T> T getObject(int i, Class<T> aClass) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(String s, Class<T> aClass) throws SQLException {
    return null;
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
