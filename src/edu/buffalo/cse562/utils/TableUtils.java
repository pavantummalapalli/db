package edu.buffalo.cse562.utils;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.ExtendedDateValue;

public final class TableUtils {
	
	private static Map <String, CreateTable> tableSchemaMap = new HashMap <>();
	private static String dataDir;
	private static String tempDataDir;
	public static boolean isSwapOn; 

	private static class TableFileFilter implements FileFilter{
		
		private String tableName;
		
		public TableFileFilter(String tableName) {
			this.tableName = tableName.toUpperCase();
		}
		
		@Override
		public boolean accept(File pathname) {
			if(pathname.isFile() && pathname.getName().toUpperCase().split("\\.")[0].equalsIgnoreCase(tableName))
				return true;
			return false;
		}
	}
	
	public static LeafValue getLeafValue(String columnName,Map<String,Integer> columnMapping,String[]colVals,CreateTable table){
		int index = columnMapping.get(columnName); 
		String value = colVals[index];
		List<ColumnDefinition> colDefns = table.getColumnDefinitions();
		ColDataType dataType = colDefns.get(index).getColDataType();
		String data = dataType.getDataType().toLowerCase();
		if(data.equalsIgnoreCase("int"))
			return new LongValue(colVals[index]);
		else if(data.equalsIgnoreCase("date")){
			if((" "+colVals[index]+" ").length()!=12)
				throw new RuntimeException("Illeagel value dates" + value);
			return new ExtendedDateValue(" "+colVals[index]+" ");
		}
		else if(data.equalsIgnoreCase("string") || data.contains("char"))
			return new StringValue(" " + colVals[index] + " ");
		else if(data.equalsIgnoreCase("double") || data.equalsIgnoreCase("decimal"))
			return new DoubleValue(colVals[index]);
		return null;
	}
	
	public static String toUnescapedString(LeafValue leafValue){
		if(leafValue instanceof StringValue){
			return leafValue.toString().substring(1,leafValue.toString().length()-1);
		}
		return leafValue.toString();
	}
	
	public static Column convertStringToColumn(String columnStr){
		String [] splitColumnNames = columnStr.split("\\.");
		Column column = new Column();
		if(splitColumnNames.length==2){
			column.setColumnName(splitColumnNames[1]);
			column.setTable(new Table());
			column.getTable().setName(splitColumnNames[0]);
		}
		else{
			column.setColumnName(splitColumnNames[0]);
			column.setTable(new Table());
		}
		return column;
	}
	
	public static File getAssociatedTableFile(String tableName){
		TableFileFilter filter = new TableFileFilter(tableName);
		File file = new File(TableUtils.getDataDir());
		File[]  files = file.listFiles(filter);
		if(files.length==0)
			throw new RuntimeException("File Not Found : " + tableName.toUpperCase());
		return files[0];
	}
	
	public static Map<String, CreateTable> getTableSchemaMap() {
		return tableSchemaMap;
	}
	
	public static List <ColumnDefinition> getColumnDefinitionForTable(String tableName,Map <String, CreateTable> tempTableSchemaMap){
		CreateTable cd = tableSchemaMap.get(tableName);
		if(cd ==null && tempTableSchemaMap !=null){
			return tempTableSchemaMap.get(tableName).getColumnDefinitions();
		}
		return cd.getColumnDefinitions();
	}

	public static void setTableSchemaMap(Map<String, CreateTable> tableSchemaMap) {
		TableUtils.tableSchemaMap = tableSchemaMap;
	}
	
	public static String getDataDir() {
		return dataDir;
	}

	public static void setDataDir(String dataDir) {
		TableUtils.dataDir = dataDir;
	}
	
	public TableUtils() {
		
	}

	public static String getTempDataDir() {
		return tempDataDir;
	}

	public static void setTempDataDir(String tempDataDir) {
		TableUtils.tempDataDir = tempDataDir;
	}
	
	public static String resolveColumnTableName(Map<String,String> columnTableMap,Column column){
		if(column.getTable() ==null || column.getTable().getName()==null || column.getTable().getName().isEmpty()){
			Table table = new Table();
			table.setName(columnTableMap.get(column.getColumnName().toUpperCase()));
			column.setTable(table);
		}
		return column.getWholeColumnName();
	}
	
	public static List<ColumnDefinition> convertColumnNameToColumnDefinitions(List<String> columnList){
		List<ColumnDefinition> defList = new ArrayList<ColumnDefinition>();
		Iterator<String> iteratorColumnList = columnList.iterator();
		while(iteratorColumnList.hasNext()){
			ColumnDefinition def =  new ColumnDefinition();
			def.setColumnName(iteratorColumnList.next());
			defList.add(def);
		}
		return defList;
	}
	
	public static List<Expression> convertSelectExpressionItemIntoExpressions(Collection<SelectExpressionItem> expressionList) {
		List<Expression> items = new ArrayList<Expression>();
		for (SelectExpressionItem expression : expressionList) {
			items.add(expression.getExpression());
		}
		return items;
	}
	
	public static List<SelectExpressionItem> convertColumnListIntoSelectExpressionItem(
			Collection<String> columnList) {
		List<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
		for (String column : columnList) {
			SelectExpressionItem item = new SelectExpressionItem();
			item.setExpression(TableUtils.convertStringToColumn(column));
			items.add(item);
		}
		return items;
	}
	
	public static List<String> convertSelectExpressionItemIntoColumnString(Collection<SelectExpressionItem> expressionList) {
		List<String> items = new ArrayList<String>();
		for (SelectExpressionItem expression : expressionList) {
			Column column;
			if(expression.getExpression() instanceof Column){
				column = (Column)expression.getExpression();
			}else{
				column = new Column();
				column.setTable(new Table());
				column.setColumnName(expression.getExpression().toString().toUpperCase());
			}
			if(expression.getAlias()!=null)
				column.setColumnName(expression.getAlias().toUpperCase());
			items.add(column.getWholeColumnName().toUpperCase());
		}
		return items;
	}
	
	public static List<Column> convertSelectExpressionItemIntoColumn(Collection<SelectExpressionItem> expressionList) {
		List<Column> items = new ArrayList<Column>();
		for (SelectExpressionItem expression : expressionList) {
			Column column;
			if(expression.getExpression() instanceof Column){
				column = (Column)expression.getExpression();
			}else{
				column = new Column();
				column.setTable(new Table());
				column.setColumnName(expression.getExpression().toString().toUpperCase());
			}
			if(expression.getAlias()!=null)
				column.setColumnName(expression.getAlias().toUpperCase());
			items.add(column);
		}
		return items;
	}
	
	public static List<ColumnDefinition> convertFunctionNameToColumnDefinitions(List<Function> functionList){
		List<ColumnDefinition> defList = new ArrayList<ColumnDefinition>();
		Iterator<Function> iteratorColumnList = functionList.iterator();
		while(iteratorColumnList.hasNext()){
			ColumnDefinition def =  new ColumnDefinition();
			def.setColumnName(iteratorColumnList.next().toString());
			defList.add(def);
		}
		return defList;
	}
	
	public static List<ColumnDefinition> convertSelectExpressionItemsIntoColumnDefinition(Collection<SelectExpressionItem> expressionList){
		return convertColumnNameToColumnDefinitions(convertSelectExpressionItemIntoColumnString(expressionList));
	}
	
	public static List<SelectExpressionItem> convertColumnDefinitionIntoSelectExpressionItems(Collection<ColumnDefinition> definitionList){
		Collection<String> columnList= new ArrayList<>();
		for(ColumnDefinition def : definitionList){
			columnList.add(def.getColumnName());
		}
		return convertColumnListIntoSelectExpressionItem(columnList);
	}

	public static List<Expression> expressionList;

	private static boolean recurse(Expression where) {
		
		if (where instanceof Parenthesis) {
			return recurse(((Parenthesis) where).getExpression());
		}
		
		if(where instanceof OrExpression){
			return false;
		}
		
		if (!(where instanceof BinaryExpression)) return true;
//		if (!(where instanceof AndExpression)) {
//			expressionList.add(where);
//			return true;
//		}
		Expression leftExpr = ((BinaryExpression)where).getLeftExpression();
		Expression rightExpr = ((BinaryExpression)where).getRightExpression();
		if ((leftExpr instanceof Column && !(rightExpr instanceof BinaryExpression)) 
				|| (!(leftExpr instanceof BinaryExpression) && rightExpr instanceof Column)) {
			expressionList.add(where);	
			return true; 
		}
		return recurse(leftExpr) && recurse(rightExpr);
	}
	
	public static List<Expression> getBinaryExpressionList(Expression where) {
		expressionList = new ArrayList<>();
		if(recurse(where))
			return expressionList;
		else
			return new ArrayList<>();
	}

	public static int compareTwoLeafValues(LeafValue leafValue1, LeafValue leafValue2) {
		int ans = 0;
		if (leafValue1 instanceof DoubleValue && leafValue2 instanceof DoubleValue)
			ans = ((DoubleValue) leafValue1).getValue() <= ((DoubleValue) leafValue2).getValue() ? -1 : 1;
		else if (leafValue1 instanceof LongValue && leafValue2 instanceof LongValue)
			ans = ((LongValue) leafValue1).getValue() <= ((LongValue) leafValue2).getValue() ? -1 : 1;
		else if (leafValue1 instanceof StringValue && leafValue2 instanceof StringValue)
			ans = ((StringValue) leafValue1).getValue().compareTo(((StringValue) leafValue2).getValue());
		else if (leafValue1 instanceof DateValue && leafValue1 instanceof DateValue)
			ans = ((DateValue) leafValue1).getValue().compareTo(((DateValue) leafValue2).getValue());
		else
			throw new RuntimeException("Its not expected. Both LeafValue should be of same type.");
		return ans;
	}

	public static String convertToString(LeafValue[] leafValueArr) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < leafValueArr.length; i++) {
			sb.append(toUnescapedString(leafValueArr[i]) + "|");
		}
		return sb.substring(0, sb.length() - 1).toString();
	}

	public static List<Expression> getIndividualJoinConditions(Expression expression) {
		List<Expression> joinExps = new ArrayList<>();
		while(expression instanceof AndExpression) {
			AndExpression andExp = (AndExpression)expression;
			joinExps.add(andExp.getLeftExpression());
			expression = andExp.getRightExpression();
		}
		joinExps.add(expression);
		return joinExps;
	}
	
}
