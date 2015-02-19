package edu.buffalo.cse562.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public final class TableUtils {
	
	private static Map <String, CreateTable> tableSchemaMap = new HashMap <>();
	private static String dataDir;

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
	
	public static String resolveColumnTableName(Map<String,String> columnTableMap,Column column){
		if(column.getTable() ==null || column.getTable().getName()==null || column.getTable().getName().isEmpty()){
			Table table = new Table();
			table.setName(columnTableMap.get(column.getColumnName()));
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
}
