package edu.buffalo.cse562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.datasource.DataSourceReader;

public class DataSourceSqlIterator implements SqlIterator {
	//Schema Info, Expression and relation to be declared
		private boolean aggregateModeOn;
		//private BufferedReader bufferedReader;
		private List <Expression> selectExpressionList;
		private CreateTable table;
		private HashMap<String, Integer> columnMapping;
		private HashMap<Integer,String> reverseColumnMapping;
		private DataSourceReader dataFileReader;
		private String[] colVals;
		private List <Column> groupByList;
		private List <ExpressionEvaluator> selectExpressionEvaluatorList;
		private ExpressionEvaluator groupByColumnEvaluator;
		private Expression filterExpression;
		private ExpressionEvaluator evaluate ;
		
		public DataSourceSqlIterator(CreateTable table, List <Expression> expression, DataSourceReader dataFile, List <Column> groupByList,Expression filterExpression) {
			groupByColumnEvaluator=new ExpressionEvaluator(table);
			this.selectExpressionList = expression;
			this.table = table;
			columnMapping = new HashMap<>();
			reverseColumnMapping = new HashMap<>();
			this.dataFileReader = dataFile;
			this.groupByList = groupByList;
			this.filterExpression=filterExpression;
			evaluate = new ExpressionEvaluator(table);
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
			List<ColumnDefinition> colDefns = table.getColumnDefinitions();
			Iterator<ColumnDefinition> iterator = colDefns.iterator();
			int index = 0;
			while(iterator.hasNext()) {
				ColumnDefinition cd = iterator.next();
				columnMapping.put(cd.getColumnName(), index);
				reverseColumnMapping.put(index, cd.getColumnName());
				index++;
			}
			if(selectExpressionList!=null && selectExpressionList.size()>0){
				selectExpressionEvaluatorList = new ArrayList<>(selectExpressionList.size());
				for (int i = 0; i < selectExpressionList.size(); i++) {
					selectExpressionEvaluatorList.add(new ExpressionEvaluator(table));
				}
			}
			evaluate = new ExpressionEvaluator(table);
		}
				
		public LeafValue[] next() {
			try {
				while(true){
					LeafValue [] convertedValues = dataFileReader.readNextTuple();
				if (convertedValues == null) {
						return null;
				}
					//TODO: detect n trim spaces
	//				colVals = row.split("\\|");
	//				LeafValue [] convertedValues = new LeafValue[colVals.length];
	//				for(int i=0;i<colVals.length;i++){
	//					Integer index  = Integer.valueOf(i);
	//					String columnName = reverseColumnMapping.get(index);
	//					convertedValues[i]=TableUtils.getLeafValue(columnName, columnMapping, colVals, table);
	//				}
					if(filterExpression!=null){
						//ExpressionEvaluator evaluate = new ExpressionEvaluator(table);
						LeafValue leafValue = evaluate.evaluateExpression(filterExpression, convertedValues, null);
						BooleanValue value =(BooleanValue) leafValue;
						if(value ==BooleanValue.FALSE)
							continue;
					}
					if(selectExpressionList ==null || selectExpressionList.size()==0){
						return convertedValues;
					}
					int count = 0;
					LeafValue[] resolvedValue = new LeafValue[convertedValues.length];
					for (Expression expression : selectExpressionList) {
						LeafValue leafValue = selectExpressionEvaluatorList.get(count).evaluateExpression(expression, convertedValues, null);
						//resolvedValues[count] =expressionEvaluatorList.get(count).getLeafValue(leafValue);
						resolvedValue[count] =leafValue;
						count++;
					}
					return resolvedValue;
				}
			}
			catch (SQLException e) {
				throw new RuntimeException("SQLException in SQLIterator next method 1", e);
				//e.printStackTrace();
			} catch (IOException e) {
				//e.printStackTrace();
				throw new RuntimeException("IOException in SQLIterator next method 2 ", e);
			}
		}
		
		public void nextAggregate() {
			try {
				LeafValue[] resolvedValues=null;

				while((resolvedValues=dataFileReader.readNextTuple()) != null){
//					colVals = row.split("\\|");
//					LeafValue [] resolvedValues = new LeafValue[colVals.length];
//					for(int i=0;i<colVals.length;i++){
//						Integer index  = Integer.valueOf(i);
//						String columnName = reverseColumnMapping.get(index);
//						resolvedValues[i]=TableUtils.getLeafValue(columnName, columnMapping, colVals, table);
//					}
					if(filterExpression!=null){
						LeafValue leafValue = evaluate.evaluateExpression(filterExpression, resolvedValues, null);
						BooleanValue value =(BooleanValue) leafValue;
						if(value ==BooleanValue.FALSE)
							continue;
					}
					GroupBy groupBy = null;
					if(groupByList!=null && groupByList.size()>0){
						LeafValue[] leafValues = new LeafValue[groupByList.size()];
						int i=0;
						for(Column groupByColumn : groupByList) {
							leafValues[i++] = groupByColumnEvaluator.evaluateExpression(groupByColumn,resolvedValues,null);
						}	
						groupBy= new GroupBy(leafValues);
					}
					if(selectExpressionList != null){
						int count = 0;
						for (Expression expression : selectExpressionList) {
							LeafValue leafValue = selectExpressionEvaluatorList.get(count).evaluateExpression(expression, resolvedValues, groupByList,groupBy);
							count++;
						}
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException("SQLException in nextAggregate method ", e);
				//e.printStackTrace();
			} catch (IOException e) {
				throw new RuntimeException("Exception while closing SQLIterator close method ", e);
				//e.printStackTrace();
			}
		}
		
		public void close() {
			try {
				dataFileReader.close();
			} catch (IOException e) {
				throw new RuntimeException("Exception while closing SQLIterator close method ", e);
			}	
		}
		
		public Map<GroupBy,Object> getAggregateData(int index) {
			return selectExpressionEvaluatorList.get(index).getCalculatedData();
		}
}
