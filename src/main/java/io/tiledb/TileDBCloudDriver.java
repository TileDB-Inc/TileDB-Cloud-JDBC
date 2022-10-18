package io.tiledb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TileDBCloudDriver implements Driver {
	private static final Driver INSTANCE = new TileDBCloudDriver();
	private static boolean registered;
	public TileDBCloudDriver() {}

	@Override
	public Connection connect(String s, Properties properties) throws SQLException {
		String[] parts = s.split(":");
		System.out.println(s);

		if (parts.length < 2 ||	!parts[0].equalsIgnoreCase("jdbc") || !parts[1].equalsIgnoreCase("tiledb-cloud"))
			return null;

		String namespace = Arrays.stream(parts).skip(2).collect(Collectors.joining(":"));

		return new TileDBCloudConnection(namespace);
	}

	@Override
	public boolean acceptsURL(String s) throws SQLException {
		return true;
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
