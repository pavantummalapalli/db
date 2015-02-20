package edu.buffalo.cse562.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;

public class AggregateTest extends BaseTest {

	private String filePath = "./query/";
	private Connection connection;
	
	@Test
	public void testAggregateFunction() throws IOException, SQLException {
		for (int i = 1; i < 13; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "AGG" + value + ".SQL";
			File file = new File(path);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.startsWith("SELECT")) {
					executeQuery(line);
				}
			}
			reader.close();
		}
	}

	private void executeQuery(String query) throws SQLException, ConnectException {		
		
		connection = getConnectionForMYSQL();
		PreparedStatement statement = connection.prepareStatement(query);
		ResultSet rs = statement.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();
		StringBuilder sb = new StringBuilder();
		int columnCount = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 0; i < columnCount; i++) {
				String columnName = rsmd.getColumnName(i + 1);
				String value = rs.getString(columnName);
				sb.append(value + "|");
			}
			if (sb.length() > 0) {
				sb = new StringBuilder(sb.substring(0, sb.length() - 1));
				sb.append("\n");
			}
		}
		System.out.println(sb);
		statement.close();
	}
}
