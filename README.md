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

Where ```NAMESPASE``` is your TileDB-Cloud namespace.

Other available properties are: ```username(String)```, ```password(String)```, ```verifySSL(boolean)```, ```overwritePrevious(boolean)```

## Run a simple query
```
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM `tiledb://TileDB-Inc/quickstart_sparse`");
```

## Limitations
Query results are limited to 2GB in size.

## Application compatibility
This driver is tested against the following applications/tools. Compatibility with others applications is not guaranteed. 
- [DBeaver](https://dbeaver.com)
- [Tableau](https://www.tableau.com) (Use with our custom [TileDB-Tableau-Connector](https://github.com/TileDB-Inc/TileDB-Tableau-Connector))
- [Microsoft Power BI](https://powerbi.microsoft.com/) (Use with the ODBC powerpack from [ZappySys](https://zappysys.com))