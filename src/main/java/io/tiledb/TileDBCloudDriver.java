package io.tiledb;

import io.tiledb.cloud.TileDBLogin;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class TileDBCloudDriver implements Driver {
	private static final Driver INSTANCE = new TileDBCloudDriver();
	private static boolean registered;
	public TileDBCloudDriver() {}

	@Override
	public Connection connect(String s, Properties properties) throws SQLException {
		if (!acceptsURL(s)) return null;

		String[] parts = s.split(":");

		//get namespace from URL
		String namespace = parts[2];

		// call the appropriate connector depending on the properties given
		if (properties.isEmpty()) return new TileDBCloudConnection(namespace);
		return new TileDBCloudConnection(namespace, createLoginObject(properties));
	}

	/**
	 * Create a TileDB login object based on the JDBC properties given as input.
	 * @param properties The properties
	 * @return The login object
	 */
	private TileDBLogin createLoginObject(Properties properties) {
		try {
			return new TileDBLogin(
					(String) properties.getOrDefault("username", null),
					(String) properties.getOrDefault("password", null),
					(String) properties.getOrDefault("apiKey", null),
					Boolean.parseBoolean((String) properties.getOrDefault("verifySSL", "true")),
					Boolean.parseBoolean((String) properties.getOrDefault("rememberMe", "false")),
					Boolean.parseBoolean((String) properties.getOrDefault("overwritePrevious", "false"))
			);
		} catch (Exception e){
			throw new RuntimeException("Error with the input properties");
		}
	}

	@Override
	public boolean acceptsURL(String s) throws SQLException {
		//check for URL correctness
		String[] parts = s.split(":");

		return parts.length >= 2 &&
				parts[0].equalsIgnoreCase("jdbc") &&
				parts[1].equalsIgnoreCase("tiledb-cloud");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 1;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	public static synchronized Driver load() {
		if (!registered) {
			registered = true;
			try {
				DriverManager.registerDriver(INSTANCE);
			} catch (SQLException throwables) {
				throwables.printStackTrace();
			}
		}

		return INSTANCE;
	}

	static {
		load();
	}
}
