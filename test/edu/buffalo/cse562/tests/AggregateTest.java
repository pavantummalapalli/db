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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import edu.buffalo.cse562.Main;

public class AggregateTest extends BaseTest {

	private static String filePath = "./query/";
	private Connection connection;
	private List <String> mysqlList = new ArrayList <>();
	private static List <String> aggregateFilePath = new ArrayList <>();
	private static List <String> groupByFilePath = new ArrayList <>();
	private static List <String> tableFuncFilePath = new ArrayList <>();
	private static List <String> unionFuncFilePath = new ArrayList <>();
	
	static {
		//AGGREGATE FILE PATH
		for (int i = 1; i < 13; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "AGG" + value + ".SQL";
			aggregateFilePath.add(path);
		}	
		
		//GROUP BY FILE PATH
		for (int i = 1; i < 13; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "GBAGG" + value + ".SQL";
			groupByFilePath.add(path);
		}	
		
		//TABLE FUNC FILE PATH
		for (int i = 1; i < 9; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "TABLE" + value + ".SQL";
			tableFuncFilePath.add(path);
		}	
		
		//UNION FILE PATH
		for (int i = 1; i < 3; i++) {
			String value = i + "";
			if (i < 10) {
				value = "0" + value;
			}
			String path = filePath + "UNION" + value + ".SQL";
			unionFuncFilePath.add(path);
		}	
	}
	
	@Test
	public void testAggregateFunction() throws IOException, SQLException {

		for (int i = 0; i < aggregateFilePath.size(); i++) {
			String path = aggregateFilePath.get(i);
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
		for (int i = 0; i < groupByFilePath.size(); i++) {
			String path = groupByFilePath.get(i);
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
		for (int i = 0; i < tableFuncFilePath.size(); i++) {
			String path = tableFuncFilePath.get(i);
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
		
		for (int i = 0; i < unionFuncFilePath.size(); i++) {
			String path = unionFuncFilePath.get(i);
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
		int columnCount = rsmd.getColumnCount();
		while (rs.next()) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < columnCount; i++) {
				String columnName = rsmd.getColumnName(i + 1);
				String value = rs.getString(columnName);
				sb.append(value + "|");
			}
			if (sb.length() > 0) {
				sb = new StringBuilder(sb.substring(0, sb.length() - 1));
			}
			mysqlList.add(sb.toString());
		}
		//System.out.println(sb);
		
		statement.close();
	}
	
	@Test
	public void testAggregateCompareWithMYSQLResults() throws Exception {
		
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    PrintStream ps = new PrintStream(baos);
	   
	    String[] args = new String[14];
	    args[0] = "--data";
	    args[1] = "./data";
	    for (int i = 0; i < aggregateFilePath.size(); i++)
	    	args[i + 2] = aggregateFilePath.get(i);
	 	
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
	    System.setOut(old);
	    old.close(); 
	    ps.close();	   
	    old.println(projectResult);
	    testAggregateFunction();
	    compareResults(projectResult, mysqlList);
	}
	
	@Test
	public void testGroupByCompareWithMYSQLResults() throws Exception {
		
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    PrintStream ps = new PrintStream(baos);
	   
	    String[] args = new String[14];
	    args[0] = "--data";
	    args[1] = "./data";
	    for (int i = 0; i < groupByFilePath.size(); i++)
	    	args[i + 2] = groupByFilePath.get(i);
	 	
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
	    System.setOut(old);
	    old.close(); 
	    ps.close();	   
	    old.println(projectResult);
	    testGroupByAggregateFunction();
	    compareResults(projectResult, mysqlList);
	}
	
	private void compareResults(List <String> projectResult, List <String> mysqlList) throws Exception {
		Collections.sort(projectResult);
		Collections.sort(mysqlList);
		if (projectResult.size() != mysqlList.size()) {
			throw new Exception("Incorrect results");
		}
		for (int i = 0; i < projectResult.size(); i++) {
			String s1 = projectResult.get(i).trim();
			String s2 = mysqlList.get(i).trim();
			if (s1.equals(s2)) continue;
			if (s1.contains(".") && s2.contains(".")) {
				if (s1.substring(0, s1.indexOf(".")).equals(s1.substring(0, s2.indexOf(".")))) {
					continue;
				}
			}
			throw new Exception("Incorrect results");
		}
	}
	
	@Test
	public void testAll() throws IOException, SQLException {
		testAggregateFunction();
		testGroupByAggregateFunction();
		testTableFunction();
		testTableFunction();
	}
}
