<a href="https://tiledb.com/"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

[![TileDB-Cloud-JDBC](https://github.com/TileDB-Inc/TileDB-Cloud-JDBC/actions/workflows/github_actions.yml/badge.svg)](https://github.com/TileDB-Inc/TileDB-Cloud-JDBC/actions/workflows/github_actions.yml)

# TileDB-Cloud JDBC Driver

This is a type 4 JDBC driver that allows a Java program to connect to TileDB-Cloud.

## Usage

### Load Driver Class

```Class.forName("io.tiledb.TileDBCloudDriver")```

## Build from source code

```./gradlew assemble```

## Authentication

Use the java.util.Properties class to add your credentials

```
Properties properties  = new Properties();
properties.setProperty("apiKey", "KEY");
properties.setProperty("rememberMe", "true");

Connection conn = DriverManager.getConnection("jdbc:tiledb-cloud:<NAMESPACE>", properties);
```

Where ```NAMESPACE``` is your TileDB-Cloud namespace.

Other available properties are: 
- ```username(String)```
- ```password(String)```
- ```verifySSL(boolean)```
- ```overwritePrevious(boolean)```

## Run a simple query
```
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM `tiledb://TileDB-Inc/quickstart_sparse`");
```

## Limitations
Query results are limited to 2GBs in size.

## Application compatibility
This driver is tested against the following applications/tools. Compatibility with other applications is not guaranteed. 
- [DBeaver](https://dbeaver.com)
- [Tableau](https://www.tableau.com) (Use with our custom [TileDB-Tableau-Connector](https://github.com/TileDB-Inc/TileDB-Tableau-Connector))
- [Microsoft Power BI](https://powerbi.microsoft.com/) (Use with the ODBC powerpack from [ZappySys](https://zappysys.com))
