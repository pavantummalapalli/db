package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class SqlIterator extends Eval {
	//Schema Info, Expression and relation to be declared
		FileReader fileReader;
		BufferedReader bufferedReader;
		List<Expression> expressions;
		CreateTable table;
		String dirPath = "D:/DB/Local/db/data/Sanity_Check_Examples/data";
		HashMap<String, Integer> columnMapping;
		String colVals[] = {};
		public SqlIterator(List<Expression> expressions, CreateTable table) {
			// Initialize Schema info vars, expressions. Extract table name and path
			//open("", "");
			this.expressions = expressions;
			this.table = table;
			columnMapping = new HashMap<>();
			open();
		}
		
		@Override
		public LeafValue eval(Column arg0) throws SQLException {
			int index = columnMapping.get(arg0.getColumnName());
			return new LongValue(colVals[index]);
		}
		
		public void open() {
			try {
				fileReader = new FileReader(dirPath + File.separator + table.getTable().getName() + ".dat");
				bufferedReader = new BufferedReader(fileReader);
				List<ColumnDefinition> colDefns = table.getColumnDefinitions();
				Iterator<ColumnDefinition> iterator = colDefns.iterator();
				int index = 0;
				while(iterator.hasNext()) {
					ColumnDefinition cd = iterator.next();
					columnMapping.put(cd.getColumnName(), index++);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		public List<LeafValue> next() {
			List<LeafValue> leafValues = new ArrayList<LeafValue>();
			try {
				String row = bufferedReader.readLine();
				if(row != null) {
					colVals = row.split("\\|");
					Iterator<Expression> iterator = expressions.iterator();
					while(iterator.hasNext()) {
						Expression expression = iterator.next();
						try {
							LeafValue leafValue = eval(expression);
							leafValues.add(leafValue);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					// Use Expression library for evaluation
					
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return leafValues;
		}
		
		public void close() {
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public static void main(String args[]) throws FileNotFoundException {
			String ipDir = "D:/DB/Local/db/data/Sanity_Check_Examples";
			File ipDirectory = new File(ipDir);
			String[] files = ipDirectory.list();
			for (String f : files) {
				System.out.println("File name : " + f);
				File file = new File(ipDir + File.separator + f);
				if(file.isFile()) {
					FileInputStream fileInputStream = new FileInputStream(file);
					CCJSqlParser parser = new CCJSqlParser(fileInputStream);
					try {
						Statement statement;
						List<Expression> expressions = new ArrayList<>();
						CreateTable createTable = null;
						while((statement = parser.Statement()) != null) {
							System.out.println("Statement : " + statement);
							if(statement instanceof CreateTable) {
								CreateTable ct = (CreateTable)statement;
								List<CreateTable> coldefns = ct.getColumnDefinitions();
								List<CreateTable> indexes = ct.getIndexes();
								Table newTable = ct.getTable();
								String alias = newTable.getAlias();
								String schemaName = newTable.getSchemaName();
								createTable = ct;
							}
							else if(statement instanceof Select) {
								Select select = (Select)statement;
								SelectBody selectBody = select.getSelectBody();
								if(selectBody instanceof PlainSelect) {
									PlainSelect plainSelect = (PlainSelect)selectBody;
									Expression expression = plainSelect.getWhere();
									if(expression != null)
										expressions.add(expression);
									System.out.println();
								}
								else if(selectBody instanceof Union) {
									Union union = (Union)selectBody;
									System.out.println();
								}
							}
						}
						SqlIterator iterator = new SqlIterator(expressions, createTable);
						iterator.next();
						iterator.close();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				
			}
		}

}
