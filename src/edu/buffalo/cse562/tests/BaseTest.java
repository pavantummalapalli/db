package edu.buffalo.cse562.tests;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.TestCase;

public abstract class BaseTest extends TestCase {
	
	private static Connection connection;
	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/dbprj1";
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "root";
	
	Connection getConnectionForMYSQL() throws ConnectException {
		if (connection != null) 
			return connection;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			throw new ConnectException("MySQL Connection failed");
		}

		try {
			connection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
		} catch (SQLException e) {
			throw new ConnectException("MySQL Connection failed");
		}

		if (connection != null) {
			System.out.println("Successfully connected");
		} else {
			System.out.println("Failed to make connection!");
			throw new ConnectException("MySQL Connection failed");
		}
		return connection;
	}
}
