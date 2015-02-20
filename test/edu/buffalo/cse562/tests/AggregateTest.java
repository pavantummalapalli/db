package edu.buffalo.cse562.tests;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.buffalo.cse562.Main;

public class AggregateTest extends BaseTest {

	private String filePath = "./query/";
	private Connection connection;
	private List <String> list;
	
	
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

	@Test
	public void testGroupByAggregateFunction() throws IOException, SQLException {
		for (int i = 1; i < 13; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "GBAGG" + value + ".SQL";
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
	
	@Test
	public void testTableFunction() throws IOException, SQLException {
		for (int i = 1; i < 9; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "TABLE" + value + ".SQL";
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
	
	@Test
	public void testUnionFunction() throws IOException, SQLException {
		
		for (int i = 1; i < 3; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "UNION" + value + ".SQL";
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
		
		list = new ArrayList <>();
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
			list.add(sb.toString());
		}
		//System.out.println(sb);
		
		statement.close();
	}
	
	@Test
	public void testAggregateCompareWithMYSQLResults() throws IOException, SQLException {
		
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    PrintStream ps = new PrintStream(baos);
	   
	    String[] args = new String[14];
	    args[0] = "--data";
	    args[1] = "./data";
	    
	    for (int i = 1; i < 13; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "AGG" + value + ".SQL";
			args[i + 1] = path;
	    }	
	    Main.main(args);
	    PrintStream old = System.out;	    
	    System.setOut(ps);	    
	    Main.main(args);	    
	    System.out.flush();
	    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));
	    List <String> projectResult = new ArrayList <>();
	    String line = "";
	    while ((line = reader.readLine()) != null) {
	    	projectResult.add(line.trim());
	    }
	    old.close();
	    ps.close();
	    System.out.println("Project Results");
	    System.out.println(projectResult);
	    testAggregateFunction();
	    System.out.println("MYSQL Results");
	    System.out.println(list);
	    System.setOut(old);
	    System.out.flush();
	}
	
	@Test
	public void testAll() throws IOException, SQLException {
		testAggregateFunction();
		testGroupByAggregateFunction();
		testTableFunction();
		testTableFunction();
	}
}