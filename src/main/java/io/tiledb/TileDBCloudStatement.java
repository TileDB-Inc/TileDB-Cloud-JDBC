package io.tiledb;

import org.openapitools.client.ApiException;
import org.openapitools.client.api.SqlApi;
import org.openapitools.client.model.ResultFormat;
import org.openapitools.client.model.SQLParameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;

public class TileDBCloudStatement implements Statement {
	private SqlApi sqlApi;

	private String namespace;

	TileDBCloudStatement(SqlApi sqlApi, String namespace) {
		this.sqlApi = sqlApi;
		this.namespace = namespace;
	}

	@Override
	public ResultSet executeQuery(String s) throws SQLException {
		try {
			SQLParameters sqlParameters = new SQLParameters();
			sqlParameters.setQuery(s);
//			sqlParameters.setParameters();
			sqlParameters.setResultFormat(ResultFormat.JSON);
			List<Object> results = sqlApi.runSQL(namespace, sqlParameters, "application/json");
			return new TileDBCloudResultSet(results, ResultFormat.JSON);
		} catch (ApiException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public int executeUpdate(String s) throws SQLException {
		return 0;
	}

	@Override
	public void close() throws SQLException {

	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxFieldSize(int i) throws SQLException {

	}

	@Override
	public int getMaxRows() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxRows(int i) throws SQLException {

	}

	@Override
	public void setEscapeProcessing(boolean b) throws SQLException {

	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return 0;
	}

	@Override
	public void setQueryTimeout(int i) throws SQLException {

	}

	@Override
	public void cancel() throws SQLException {

	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {

	}

	@Override
	public void setCursorName(String s) throws SQLException {

	}

	@Override
	public boolean execute(String s) throws SQLException {
		return false;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return null;
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
	public void setFetchDirection(int i) throws SQLException {

	}

	@Override
	public int getFetchDirection() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchSize(int i) throws SQLException {

	}

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
	public void addBatch(String s) throws SQLException {

	}

	@Override
	public void clearBatch() throws SQLException {

	}

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
		return false;
	}

	@Override
	public boolean execute(String s, String[] strings) throws SQLException {
		return false;
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
	public void setPoolable(boolean b) throws SQLException {

	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public void closeOnCompletion() throws SQLException {

	}

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
