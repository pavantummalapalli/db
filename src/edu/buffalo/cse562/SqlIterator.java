package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.utils.TableUtils;

public class SqlIterator extends Eval {
	//Schema Info, Expression and relation to be declared
		FileReader fileReader;
		BufferedReader bufferedReader;
		Expression expression;
		CreateTable table;
		HashMap<String, Integer> columnMapping;
		File dataFile;
		private String[] colVals;
		
		public SqlIterator(CreateTable table, Expression expression, File dataFile) {
			this.expression = expression;
			this.table = table;
			columnMapping = new HashMap<>();
			this.dataFile = dataFile;
			open();
		}
		
		public String[] getColVals() {
			return colVals;
		}
		
		public void setColVals(String[] colVals) {
			this.colVals = colVals;
		}
		
		@Override
		public LeafValue eval(Column arg0) throws SQLException {
			int index = columnMapping.get(arg0.getWholeColumnName());
			List<ColumnDefinition> colDefns = table.getColumnDefinitions();
			ColDataType dataType = colDefns.get(index).getColDataType();
			if(dataType.getDataType().equals("int"))
				return new LongValue(colVals[index]);
			else if(dataType.getDataType().equals("date"))
				return new DateValue(colVals[index]);
			else if(dataType.getDataType().equals("string"))
				return new StringValue(colVals[index]);
			else if(dataType.getDataType().equals("double"))
				return new DoubleValue(colVals[index]);
			return null;
		}
		
		public void open() {
			try {
				String tableName = table.getTable().getName();
				fileReader = new FileReader(dataFile);
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
		
		public String[] next() {
			try {
				String row = bufferedReader.readLine();
				if(row == null || row.trim().isEmpty())
					return null;
				//TODO: detect n trim spaces
				colVals = row.split("\\|");
				try {
					if(expression != null) {
						LeafValue leafValue = eval(expression);
						if(leafValue instanceof BooleanValue) {
							BooleanValue booleanValue = (BooleanValue)leafValue;
							if(booleanValue == BooleanValue.FALSE) {
								String arr[] = {};
								return arr;
							}
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return colVals;
		}
		
		public void close() {
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/*public static void main(String args[]) throws FileNotFoundException {
			String ipDir = "D:/DB/Local/db/data/Sanity_Check_Examples/111.SQL";
			File file = new File(ipDir);
			if(file.isFile()) {
				FileInputStream fileInputStream = new FileInputStream(file);
				CCJSqlParser parser = new CCJSqlParser(fileInputStream);
				try {
					Statement statement;
					Expression expression = null;
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
								expression = plainSelect.getWhere();
								System.out.println();
							}
							else if(selectBody instanceof Union) {
								Union union = (Union)selectBody;
								System.out.println();
							}
						}
					}
					TableUtils.setDataDir("D:/DB/Local/db/data/Sanity_Check_Examples/data");
					SqlIterator iterator = new SqlIterator(createTable, expression);
					List<String> values;
					while((values = iterator.next()) != null) {
						System.out.println(values);
					}
					iterator.close();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}*/

}
