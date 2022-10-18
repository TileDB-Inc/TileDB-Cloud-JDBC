package io.tiledb;

import org.openapitools.client.ApiClient;
import org.openapitools.client.ApiException;
import org.openapitools.client.Configuration;
import org.openapitools.client.api.ArrayApi;
import org.openapitools.client.auth.ApiKeyAuth;
import org.openapitools.client.model.ArrayBrowserData;
import org.openapitools.client.model.ArrayInfo;
import org.openapitools.client.api.SqlApi;
import org.openapitools.client.model.FileType;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class TileDBCloudConnection implements java.sql.Connection {
	private ArrayApi arrayApi;
	private SqlApi sqlApi;

	private String namespace;

	TileDBCloudConnection(String namespace) {
		//todo change to use new login when update is out
		ApiClient defaultClient = Configuration.getDefaultApiClient();
		defaultClient.setBasePath("https://api.tiledb.com/v1");

		Map<String, String> env = System.getenv();

		// Configure API key authorization: ApiKeyAuth
		ApiKeyAuth ApiKeyAuth = (ApiKeyAuth) defaultClient.getAuthentication("ApiKeyAuth");
		ApiKeyAuth.setApiKey(env.get("TILEDB_REST_TOKEN"));

		arrayApi = new ArrayApi(defaultClient);
		sqlApi = new SqlApi(defaultClient);

		this.namespace = namespace;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new TileDBCloudStatement(sqlApi, namespace);
	}

	@Override
	public PreparedStatement prepareStatement(String s) throws SQLException {
		return null;
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
	public void setAutoCommit(boolean b) throws SQLException {

	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return false;
	}

	@Override
	public void commit() throws SQLException {

	}

	@Override
	public void rollback() throws SQLException {

	}

	@Override
	public void close() throws SQLException {

	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {

		// List arrays owned
		try {
			List<String> excludeFileType = Arrays.asList(FileType.NOTEBOOK.toString(), FileType.FILE.toString(), FileType.ML_MODEL.toString(), FileType.REGISTERED_TASK_GRAPH.toString(), FileType.USER_DEFINED_FUNCTION.toString());
			ArrayBrowserData result = arrayApi.arraysBrowserOwnedGet(null, null, null, namespace, null, null, null, null, null, excludeFileType, null);
			System.out.println(result); //todo handle metadata

//			DatabaseMetaData databaseMetaData;
//			databaseMetaData.getTables();
		} catch (ApiException e) {
			System.err.println("Exception when calling ArrayApi#getArraysInNamespace");
			System.err.println("Status code: " + e.getCode());
			System.err.println("Reason: " + e.getResponseBody());
			System.err.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void setReadOnly(boolean b) throws SQLException {

	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String s) throws SQLException {

	}

	@Override
	public String getCatalog() throws SQLException {
		return null;
	}

	@Override
	public void setTransactionIsolation(int i) throws SQLException {

	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {

	}

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
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

	}

	@Override
	public void setHoldability(int i) throws SQLException {

	}

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
	public void rollback(Savepoint savepoint) throws SQLException {

	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {

	}

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
	public void setClientInfo(String s, String s1) throws SQLClientInfoException {

	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {

	}

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
	public void setSchema(String s) throws SQLException {

	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {

	}

	@Override
	public void setNetworkTimeout(Executor executor, int i) throws SQLException {

	}

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
