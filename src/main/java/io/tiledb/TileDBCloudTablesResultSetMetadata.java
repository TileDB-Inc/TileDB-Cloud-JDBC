package io.tiledb;

import io.tiledb.cloud.rest_api.model.ArrayInfo;
import io.tiledb.util.Util;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.logging.Logger;

public class TileDBCloudTablesResultSetMetadata implements ResultSetMetaData {
  private Logger logger = Logger.getLogger(TileDBCloudTablesResultSetMetadata.class.getName());

  private List<ArrayInfo> arraysOwned;
  private List<ArrayInfo> arraysShared;
  private List<ArrayInfo> arraysPublic;
  private int numberOfArrays;

  public TileDBCloudTablesResultSetMetadata(
      List<ArrayInfo> arraysOwned,
      List<ArrayInfo> arraysShared,
      List<ArrayInfo> arraysPublic,
      int numberOfArrays) {
    this.arraysOwned = arraysOwned;
    this.arraysShared = arraysShared;
    this.arraysPublic = arraysPublic;
    this.numberOfArrays = numberOfArrays;
  }

  @Override
  public int getColumnCount() throws SQLException {
    if (this.numberOfArrays == 0) return 0;
    return 5; // refers to the number of column Labels from TileDBCloudTablesResultSet which are 5
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
    return this.getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if (column < arraysOwned.size()) {
      return arraysOwned.get(column - 1).getName();
    }

    if (column - arraysOwned.size() < arraysShared.size()) {
      return arraysShared.get(column - arraysOwned.size() - 1).getName();
    }

    if (column - arraysOwned.size() - arraysShared.size() < arraysPublic.size()) {
      return arraysPublic.get(column - arraysOwned.size() - arraysShared.size() - 1).getName();
    }

    return "";
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
    return "TileDB-Arrays";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "TiledDB-Catalog";
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
