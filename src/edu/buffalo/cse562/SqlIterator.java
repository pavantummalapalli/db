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

public class SqlIterator extends Eval {
	//Schema Info, Expression and relation to be declared
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
		
		public String[] getColVals() {
			return colVals;
		}
		
		public void setColVals(String[] colVals) {
			this.colVals = colVals;
		}
		
		@Override
		public LeafValue eval(net.sf.jsqlparser.expression.Function function) throws SQLException {
			if(function.getName().equalsIgnoreCase("SUM")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				String eval = getLeafValue(eval(exp));
				
			}
			else if(function.getName().equalsIgnoreCase("AVG")){
				
			}
			return super.eval(function);
		}
		
		@Override
		public LeafValue eval(Column arg0) throws SQLException {			
			int index = columnMapping.get(arg0.getWholeColumnName());
			List<ColumnDefinition> colDefns = table.getColumnDefinitions();
			ColDataType dataType = colDefns.get(index).getColDataType();
			if(dataType.getDataType().equalsIgnoreCase("int"))
				return new LongValue(colVals[index]);
			else if(dataType.getDataType().equalsIgnoreCase("date"))
				return new DateValue(colVals[index]);
			else if(dataType.getDataType().equalsIgnoreCase("string"))
				return new StringValue(colVals[index]);
			else if(dataType.getDataType().equalsIgnoreCase("double"))
				return new DoubleValue(colVals[index]);
			return null;
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
				e.printStackTrace();
			}
		}
		
		public String getLeafValue(LeafValue leafValue) {
			if (leafValue instanceof DoubleValue)
				return String.valueOf(((DoubleValue) leafValue).getValue());
			else if (leafValue instanceof LongValue)
				return String.valueOf(((LongValue) leafValue).getValue());
			else if (leafValue instanceof StringValue)
				return ((StringValue) leafValue).getValue();
			else if (leafValue instanceof DateValue)
				return String.valueOf(((DateValue) leafValue).getDate());
			//TODO throw Unsupported 
			return "";
		}
		public String getLeafValue(LongValue leafValue) {
			return String.valueOf(leafValue.getValue());
		}
		public String getLeafValue(StringValue leafValue) {
			return leafValue.getValue();
		}
		public String getLeafValue(DateValue leafValue) {
			return String.valueOf(leafValue.getDate());
		}
		
		public String[] next() {
			try {
				String row = bufferedReader.readLine();
				if(row == null || row.trim().isEmpty())
					return null;
				//TODO: detect n trim spaces
				colVals = row.split("\\|");
				try {
					
					if(expressionList != null){
						int count = 0;
						LeafValue leafValue =expressionEvaluatorList.get(count).evaluateExpression(expressionList.get(count), colVals, groupByList);
						if(leafValue instanceof BooleanValue) {
							BooleanValue booleanValue = (BooleanValue)leafValue;
							if(booleanValue == BooleanValue.FALSE) {
								String arr[] = {};
								return arr;
							}
						} else {
							String[] arr = new String[1];
							arr[0] = getLeafValue(leafValue);
							return arr;
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
		
		public Map<String,Object> getAggregateData(int index) {
			return expressionEvaluatorList.get(index).getCalculatedData();
		}
}
