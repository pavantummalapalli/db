package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SqlIterator {
	//Schema Info, Expression and relation to be declared
		FileReader fileReader;
		BufferedReader bufferedReader;
		List<Expression> expressions;
		CreateTable table;
		String dirPath;
		public SqlIterator(List<Expression> expressions, CreateTable table) {
			// Initialize Schema info vars, expressions. Extract table name and path
			//open("", "");
			this.expressions = expressions;
			this.table = table;
			open();
		}
		
		public void open() {
			try {
				fileReader = new FileReader(dirPath + File.separator + table.getTable().getName());
				bufferedReader = new BufferedReader(fileReader);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		public String[] next() {
			String columns[] = {};
			try {
				String row = bufferedReader.readLine();
				if(row != null) {
					columns = row.split("|");
					// Use Expression library for evaluation
					
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return columns;
		}
		
		public void close() {
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/*public void test() {
			CCJSqlParser parser = new CCJSqlParser(fileReader);
			try {
				Statement statement;
				while((statement = parser.Statement()) != null) {
					System.out.println("Statement : " + statement);
					if(statement instanceof CreateTable) {
						CreateTable ct = (CreateTable)statement;
						List<CreateTable> coldefns = ct.getColumnDefinitions();
						List<CreateTable> indexes = ct.getIndexes();
						Table newTable = ct.getTable();
						String alias = newTable.getAlias();
						String schemaName = newTable.getSchemaName();
					}
					else if(statement instanceof Select) {
						Select select = (Select)statement;
						SelectBody selectBody = select.getSelectBody();
						if(selectBody instanceof PlainSelect) {
							PlainSelect plainSelect = (PlainSelect)selectBody;
							Expression expression = plainSelect.getWhere();
							System.out.println();
						}
						else if(selectBody instanceof Union) {
							Union union = (Union)selectBody;
							System.out.println();
						}
						List<Select> list = select.getWithItemsList();
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}*/
		
		public static void main(String args[]) {
			String ipDir = "D:/DB/Local/db/data/Sanity_Check_Examples";
			
			File ipDirectory = new File(ipDir);
			String[] files = ipDirectory.list();
			for (String f : files) {
				System.out.println("File name : " + f);
				/*SqlIterator iterator = new SqlIterator();
				iterator.open(f, ipDir);
				//iterator.test();
				iterator.close();*/
			}
		}
}
