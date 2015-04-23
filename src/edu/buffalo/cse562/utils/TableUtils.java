package edu.buffalo.cse562.utils;

import java.io.File;
import java.io.FileFilter;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;

import edu.buffalo.cse562.ExpressionEvaluator;
import edu.buffalo.cse562.ExpressionTriplets;
import edu.buffalo.cse562.ExtendedDateValue;
import edu.buffalo.cse562.berkelydb.IndexMetaData;

public final class TableUtils {
	
	public static Pattern pattern = Pattern.compile("\\|");
	private static Map <String, CreateTable> tableSchemaMap = new HashMap <>();
	private static String dataDir;
	private static String tempDataDir;
    private static String dbDir;
	public static boolean isSwapOn=false;
    public static boolean isLoadPhase = false;
	private static Map<String,DateValue> pooledDateValue = new HashMap<String, DateValue>();
	public static Map<String,IndexMetaData> tableIndexMetaData = new HashMap<>();

    public static String getDbDir() {
        return dbDir;
    }

    public static void setDbDir(String dbDir) {
        TableUtils.dbDir = dbDir;
    }
    
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
	
    public static void bindLeafValueToKey(LeafValue leafValue, DatabaseEntry key) {
		if(leafValue instanceof LongValue){
			try {
				TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(leafValue.toLong(), key);
			} catch (InvalidLeaf e) {
				e.printStackTrace();
			}
		}else if(leafValue instanceof StringValue){
			TupleBinding.getPrimitiveBinding(String.class).objectToEntry(((StringValue) leafValue).getValue(), key);
		}
		else if(leafValue instanceof DoubleValue){
			TupleBinding.getPrimitiveBinding(Double.class).objectToEntry(((DoubleValue)leafValue).toDouble(), key);
		}
		else if(leafValue instanceof DateValue){
			TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(((DateValue)leafValue).getValue().getTime(), key);
		}
	}
    
    public static Map<String,String> getColumnTableMap(List<ColumnDefinition> colDefList,Table table){
    	Map<String,String> columnTableMap = new HashMap<>(); 
		for (ColumnDefinition columnDef : colDefList) {
			columnTableMap.put(columnDef.getColumnName().toUpperCase(), table.getAlias().toUpperCase());
		}
		return columnTableMap;
    }
    
	public static Map<String,Integer> getColumnMapping(List<ColumnDefinition> colDefns){
		Map<String,Integer> columnMapping = new HashMap<String, Integer>();
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while(iterator.hasNext()) {
			ColumnDefinition cd = iterator.next();
			columnMapping.put(cd.getColumnName(), index);
			index++;
		}
		return columnMapping;
	}
	
	public static LeafValue getIdentifiedLeafValue(Object val){
		if(val instanceof String) 
			return new StringValue(" " + val.toString() + " ");
		else if(val instanceof Long || val instanceof Integer)
			return new LongValue(val.toString());
		else if(val instanceof Double)
			return new DoubleValue(val.toString());
		else if(val instanceof DateValue || val instanceof Date)
			return TableUtils.getPooledDateValue("'"+val.toString()+"'");
		return null;
	}
	
	//Optimize this function more
	public static LeafValue getLeafValue(String columnName,Map<String,Integer> columnMapping,String[]colVals,CreateTable table){
		return getLeafValue(columnName, columnMapping, colVals, table.getColumnDefinitions());
	}
	
	public static LeafValue getLeafValue(String columnName,Map<String,Integer> columnMapping,String[]colVals,List<ColumnDefinition> colDefns){
		int index = columnMapping.get(columnName);
		String value = colVals[index];
		ColDataType dataType = colDefns.get(index).getColDataType();
		String data = dataType.getDataType();
		if(data.equals("INT") || data.equals("int"))
			return new LongValue(colVals[index]);
		else if(data.equals("DATE") || data.equals("date")){
			if((colVals[index]).length()!=10)
				throw new RuntimeException("Illeagel value dates" + value);
			return TableUtils.getPooledDateValue("'"+colVals[index]+"'");
		}
		else if(data.contains("CHAR") || data.contains("char") || data.equals("STRING") || data.equals("string"))
			return new StringValue(" " + colVals[index] + " ");
		else if(data.equals("DECIMAL") || data.equals("decimal") || data.equals("DOUBLE") || data.equals("double"))
			return new DoubleValue(colVals[index]);
		return null;
	}
	
//	public static LeafValue getLeafValue(String columnName,Map<String,Integer> columnMapping,LeafValue[]colVals,CreateTable table){
//		int index = columnMapping.get(columnName); 
//		return colVals[index];
//	}
	
	public static String toUnescapedString(LeafValue leafValue){
		if(leafValue instanceof StringValue){
			return ((StringValue) leafValue).getValue();
		}
		return leafValue.toString();
	}
	
	public static Column convertStringToColumn(String columnStr){
		String [] splitColumnNames = columnStr.split("\\.");
		//String [] splitColumnNames = pattern.split(columnStr, 0);
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
	
	public static DateValue getPooledDateValue(String parameter){
		DateValue dateValue=pooledDateValue.get(parameter);
		if(dateValue!=null){
			return dateValue;
		}
		else{
			dateValue=new ExtendedDateValue(parameter);
			pooledDateValue.put(parameter, dateValue);
			return dateValue;
		}
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

	private static boolean recurse(Expression where,List<Expression> expressionList) {
		
		if (where instanceof Parenthesis) {
			List<Expression> tempExpression = new ArrayList<Expression>();
			if(recurse(((Parenthesis) where).getExpression(), tempExpression)){
				expressionList.addAll(tempExpression);
			}
			else
				expressionList.add(where);
			return true;
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
		return recurse(leftExpr,expressionList) && recurse(rightExpr,expressionList);
	}
	
	public static List<Expression> getBinaryExpressionList(Expression where) {
		List<Expression> expressionList = new ArrayList<>();
		if(recurse(where,expressionList))
			return expressionList;
		else
			return new ArrayList<>();
	}

	public static int compareTwoLeafValues(LeafValue leafValue1, LeafValue leafValue2) {
		int ans = 0;
		if (leafValue1 instanceof DoubleValue && leafValue2 instanceof DoubleValue)
			ans = new Double(((DoubleValue) leafValue1).getValue()).compareTo(new Double(((DoubleValue) leafValue2).getValue()));
		else if (leafValue1 instanceof LongValue && leafValue2 instanceof LongValue)
			ans = new Long(((LongValue) leafValue1).getValue()).compareTo(new Long(((LongValue) leafValue2).getValue()));
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
		if(expression != null)
			joinExps.add(expression);
		return joinExps;
	}

	public static int compareTwoLeafValuesList(LeafValue[] colVals1, LeafValue[] colVals2, List<Integer>[] columnIndexList) {
		int res = 0;
		for (int i = 0; i < columnIndexList[0].size(); i++) {
			int col1 = columnIndexList[0].get(i);
			int col2 = columnIndexList[1].get(i);
			res = compareTwoLeafValues(colVals1[col1], colVals2[col2]);
			if (res == 0) continue;
			return res;
		}
		return res;
	}
	
	public static Long getAvailableMemory(){
		Long maxMem=Runtime.getRuntime().maxMemory();
		Long freeMem=Runtime.getRuntime().freeMemory();
		Long totMem=Runtime.getRuntime().totalMemory();
		Long totalFreeMem = maxMem - (totMem - freeMem);
		return totalFreeMem;
	}
	
	public static Long getAvailableMemoryInKB(){
		return getAvailableMemory()/1024;
	}
	
	public static Long getAvailableMemoryInMB(){
		return getAvailableMemoryInKB()/1024;
	}

    //TODO caching
    public static DateValue getDateValueFromLongValue(long value) {
        Date date = new Date(value);
        DateValue dateValue = new DateValue("'1970-01-01'");
        dateValue.setValue(date);
        return dateValue;
    }

    private static void getIndexableColumns(Expression where, List<ExpressionTriplets> columnList) throws SQLException {
        if (where == null || where instanceof  Column || where instanceof LeafValue) return;

        Expression leftExpression = ((BinaryExpression)where).getLeftExpression();
        Expression rightExpression = ((BinaryExpression)where).getRightExpression();

        if ( (leftExpression instanceof Column)) {
            if (rightExpression instanceof LeafValue) {
                columnList.add(new ExpressionTriplets((Column) leftExpression, (BinaryExpression) where, (LeafValue) rightExpression));
                return;
            }
            if (rightExpression instanceof Function) {
                CreateTable newTable = new CreateTable();
                newTable.setColumnDefinitions(Arrays.asList(new ColumnDefinition()));
                ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator(newTable);
                LeafValue evaluatedLeafValue = expressionEvaluator.eval(rightExpression);
                columnList.add(new ExpressionTriplets((Column)leftExpression, (BinaryExpression)where, evaluatedLeafValue));
            }
        }
        getIndexableColumns(leftExpression, columnList);
        getIndexableColumns(rightExpression, columnList);
    }

    public static List<ExpressionTriplets> getIndexableColumns(Expression where) throws SQLException {
        List<ExpressionTriplets> indexableColumnsList = new ArrayList<>();
        getIndexableColumns(where, indexableColumnsList);
        return indexableColumnsList;
    }
}
