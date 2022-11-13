package io.tiledb;

import io.tiledb.util.Util;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.logging.Logger;

public class TileDBCloudSchemasResultSetMetadata implements ResultSetMetaData {
  private Logger logger = Logger.getLogger(TileDBCloudSchemasResultSetMetadata.class.getName());

  public TileDBCloudSchemasResultSetMetadata() {}

  @Override
  public int getColumnCount() throws SQLException {
    return 1;
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
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return "";
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return Util.SCHEMA_NAME;
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
    return "";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "TileDB-Catalog";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return Types.VARCHAR;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return "Varchar";
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
    return "Varchar";
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
