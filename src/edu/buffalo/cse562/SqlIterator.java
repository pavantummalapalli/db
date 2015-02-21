package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SqlIterator {
	//Schema Info, Expression and relation to be declared
		private boolean aggregateModeOn;
		FileReader fileReader;
		BufferedReader bufferedReader;
		List <Expression> expressionList;
		CreateTable table;
		HashMap<String, Integer> columnMapping;
		File dataFile;
		private String[] colVals;
		private List <String> groupByList;
		private List <ExpressionEvaluator> expressionEvaluatorList;
			
		public SqlIterator(CreateTable table, List <Expression> expression, File dataFile, List <String> groupByList) {
			this.expressionList = expression;
			this.table = table;
			columnMapping = new HashMap<>();
			this.dataFile = dataFile;
			this.groupByList = groupByList;
			
			open();
		}
		
		public void setAggregateModeOn(boolean aggregateModeOn) {
			this.aggregateModeOn = aggregateModeOn;
		}
		
		public boolean isAggregateModeOn() {
			return aggregateModeOn;
		}
		
		public String[] getColVals() {
			return colVals;
		}
		
		public void setColVals(String[] colVals) {
			this.colVals = colVals;
		}
		
		public void open() {
			try {
				fileReader = new FileReader(dataFile);
				bufferedReader = new BufferedReader(fileReader);
				List<ColumnDefinition> colDefns = table.getColumnDefinitions();
				Iterator<ColumnDefinition> iterator = colDefns.iterator();
				int index = 0;
				while(iterator.hasNext()) {
					ColumnDefinition cd = iterator.next();
					columnMapping.put(cd.getColumnName(), index++);
				}
				if(expressionList!=null && expressionList.size()>0){
					expressionEvaluatorList = new ArrayList<>(expressionList.size());
					for (int i = 0; i < expressionList.size(); i++) {
						expressionEvaluatorList.add(new ExpressionEvaluator(table));
					}
				}
				//expressionEvaluator = new ExpressionEvaluator(table);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		
		
		public String[] next() {
			try {
				String row = bufferedReader.readLine();
				if(row == null || row.trim().isEmpty())
					return null;
				//TODO: detect n trim spaces
				colVals = row.split("\\|");
				if(expressionList ==null || expressionList.size()==0)
					return colVals;
				String [] resolvedValues = new String[expressionList.size()];
				int count = 0;
				for (Expression expression : expressionList) {
					LeafValue leafValue = expressionEvaluatorList.get(count).evaluateExpression(expression, colVals, null);
					resolvedValues[count] =expressionEvaluatorList.get(count).getLeafValue(leafValue);
					count++;
				}
				return resolvedValues;
			}
			catch (SQLException e) {
				throw new RuntimeException("SQLException in SQLIterator next method 1", e);
				//e.printStackTrace();
			} catch (IOException e) {
				//e.printStackTrace();
				throw new RuntimeException("IOException in SQLIterator next method 2 ", e);
			} 
		}
		
		public String[] nextAggregate() {
			try {
				String row = bufferedReader.readLine();
				if(row == null || row.trim().isEmpty())
					return null;
				//TODO: detect n trim spaces
				colVals = row.split("\\|");
				try {
					
					if(expressionList != null){
						int count = 0;
						for (Expression expression : expressionList) {
							LeafValue leafValue = expressionEvaluatorList.get(count).evaluateExpression(expression, colVals, groupByList);
							count++;
						}	
					}
				} catch (SQLException e) {
					throw new RuntimeException("SQLException in nextAggregate method ", e);
					//e.printStackTrace();
				}
			} catch (IOException e) {
				throw new RuntimeException("Exception while closing SQLIterator close method ", e);
				//e.printStackTrace();
			}
			
			return colVals;
		}
		
		public void close() {
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				throw new RuntimeException("Exception while closing SQLIterator close method ", e);
			}	
		}
		
		public Map<String,Object> getAggregateData(int index) {
			return expressionEvaluatorList.get(index).getCalculatedData();
		}
}
