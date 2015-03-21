package edu.buffalo.cse562;

import java.io.BufferedReader;
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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.queryplan.DataSource;
import edu.buffalo.cse562.utils.TableUtils;

public class DataSourceSqlIterator implements SqlIterator {
	//Schema Info, Expression and relation to be declared
		private boolean aggregateModeOn;
		private BufferedReader bufferedReader;
		private List <Expression> selectExpressionList;
		private CreateTable table;
		private HashMap<String, Integer> columnMapping;
		private HashMap<Integer,String> reverseColumnMapping;
		private DataSource dataFile;
		private String[] colVals;
		private List <String> groupByList;
		private List <ExpressionEvaluator> selectExpressionEvaluatorList;
		private Expression filterExpression;
		private ExpressionEvaluator evaluate;
		
		public DataSourceSqlIterator(CreateTable table, List <Expression> expression, DataSource dataFile, List <String> groupByList,Expression filterExpression) {
			this.selectExpressionList = expression;
			this.table = table;
			columnMapping = new HashMap<>();
			reverseColumnMapping = new HashMap<>();
			this.dataFile = dataFile;
			this.groupByList = groupByList;
			this.filterExpression=filterExpression;
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
				//fileReader = new FileReader(dataFile);
				bufferedReader = new BufferedReader(dataFile.getReader());
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
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			evaluate = new ExpressionEvaluator(table);
		}
				
		public LeafValue[] next() {
			try {
				String row = bufferedReader.readLine();
				if(row == null || row.trim().isEmpty())
					return null;
				//TODO: detect n trim spaces
				colVals = row.split("\\|");
				if(filterExpression!=null){
					//ExpressionEvaluator evaluate = new ExpressionEvaluator(table);
					LeafValue leafValue = evaluate.evaluateExpression(filterExpression, colVals, null);
					BooleanValue value =(BooleanValue) leafValue;
					if(value ==BooleanValue.FALSE)
						return next();
				}
				if(selectExpressionList ==null || selectExpressionList.size()==0){
					LeafValue [] resolvedValues = new LeafValue[colVals.length];
					for(int i=0;i<colVals.length;i++){
						Integer index  = Integer.valueOf(i);
						String columnName = reverseColumnMapping.get(index);
						resolvedValues[i]=TableUtils.getLeafValue(columnName, columnMapping, colVals, table);
					}
					return resolvedValues;
				}
				LeafValue [] resolvedValues = new LeafValue[selectExpressionList.size()];
				int count = 0;
				for (Expression expression : selectExpressionList) {
					LeafValue leafValue = selectExpressionEvaluatorList.get(count).evaluateExpression(expression, colVals, null);
					//resolvedValues[count] =expressionEvaluatorList.get(count).getLeafValue(leafValue);
					resolvedValues[count] =leafValue;
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
		
		public void nextAggregate() {
			try {
				String row = null;
				while((row=bufferedReader.readLine()) != null && !row.trim().isEmpty()){
					colVals = row.split("\\|");
					if(filterExpression!=null){
						//ExpressionEvaluator evaluate = new ExpressionEvaluator(table);
						LeafValue leafValue = evaluate.evaluateExpression(filterExpression, colVals, null);
						BooleanValue value =(BooleanValue) leafValue;
						if(value ==BooleanValue.FALSE)
							continue;
					}
					if(selectExpressionList != null){
						int count = 0;
						for (Expression expression : selectExpressionList) {
							LeafValue leafValue = selectExpressionEvaluatorList.get(count).evaluateExpression(expression, colVals, groupByList);
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
				bufferedReader.close();
			} catch (IOException e) {
				throw new RuntimeException("Exception while closing SQLIterator close method ", e);
			}	
		}
		
		public Map<String,Object> getAggregateData(int index) {
			return selectExpressionEvaluatorList.get(index).getCalculatedData();
		}
}
