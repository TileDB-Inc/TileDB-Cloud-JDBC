package io.tiledb;

import io.tiledb.cloud.rest_api.model.ArrayInfo;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.logging.Logger;

public class TileDBCloudTablesResultSetMetadata implements ResultSetMetaData {
  private Logger logger = Logger.getLogger(TileDBCloudTablesResultSetMetadata.class.getName());

  private List<ArrayInfo> arrays;

  public TileDBCloudTablesResultSetMetadata(List<ArrayInfo> arrays) {
    this.arrays = arrays;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return arrays.size();
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
    return 0;
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
    return null;
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return arrays.get(column - 1).getName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "TileDB-Schema";
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
    return arrays.get(column - 1).getName();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return null;
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
    return false;
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
