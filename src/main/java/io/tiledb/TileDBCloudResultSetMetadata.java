package io.tiledb;

import io.tiledb.util.Util;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.logging.Logger;
import org.apache.arrow.vector.*;

public class TileDBCloudResultSetMetadata implements ResultSetMetaData {

  private Logger logger = Logger.getLogger(TileDBCloudResultSetMetadata.class.getName());
  private int columnCount;
  private ArrayList<ValueVector> valueVectors;

  private String tableName, namespace;

  @Override
  public int getColumnCount() throws SQLException {
    return this.columnCount;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return true; // todo
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 20;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return this.getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return valueVectors.get(column - 1).getName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return Util.SCHEMA_NAME;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return "TileDB-Array"; // todo
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return this.namespace;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    ValueVector valueVector = valueVectors.get(column - 1);
    if (valueVector instanceof IntVector || valueVector instanceof UInt2Vector) {
      return Types.INTEGER;
    } else if (valueVector instanceof Float4Vector) {
      return Types.FLOAT;
    } else if (valueVector instanceof Float8Vector) {
      return Types.DOUBLE;
    } else if (valueVector instanceof BigIntVector || valueVector instanceof UInt4Vector) {
      return Types.BIGINT;
    } else if (valueVector instanceof UInt1Vector) {
      return Types.SMALLINT;
    } else if (valueVector instanceof TinyIntVector) {
      return Types.TINYINT;
    } else if (valueVector instanceof VarCharVector) {
      return Types.VARCHAR;
    } else if (valueVector instanceof TimeStampVector) {
      return Types.TIMESTAMP_WITH_TIMEZONE;
    }
    throw new RuntimeException(
        "Type for column with index: " + column + " is not yet supported by this driver.");
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return this.getColumnClassName(column);
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    ValueVector valueVector = valueVectors.get(column - 1);
    if (valueVector instanceof IntVector || valueVector instanceof UInt2Vector) {
      return Integer.class.getName();
    } else if (valueVector instanceof Float4Vector) {
      return Float.class.getName();
    } else if (valueVector instanceof Float8Vector) {
      return Double.class.getName();
    } else if (valueVector instanceof BigIntVector || valueVector instanceof UInt4Vector) {
      return Long.class.getName();
    } else if (valueVector instanceof UInt1Vector) {
      return Short.class.getName();
    } else if (valueVector instanceof TinyIntVector) {
      return Byte.class.getName();
    } else if (valueVector instanceof VarCharVector) {
      return String.class.getName();
    } else if (valueVector instanceof TimeStampVector) {
      return Timestamp.class.getName();
    }
    throw new RuntimeException(
        "Type for column with index: " + column + " is not yet supported by this driver.");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  public void setColumnCount(int columnCount) {
    this.columnCount = columnCount;
  }

  public void setValueVectors(ArrayList<ValueVector> valueVectors) {
    this.valueVectors = valueVectors;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }
}
