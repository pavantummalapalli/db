package edu.buffalo.cse562.utils;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public final class TableUtils {
	
	private static Map <String, CreateTable> tableSchemaMap = new HashMap <>();
	private static String dataDir;

	public static Map<String, CreateTable> getTableSchemaMap() {
		return tableSchemaMap;
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
	
	private TableUtils() {
		
	}
	
	public static String resolveColumnTableName(Map<String,String> columnTableMap,Column column){
		if(column.getTable() ==null || column.getTable().getName()==null || column.getTable().getName().isEmpty()){
			column.getTable().setName(columnTableMap.get(column.getColumnName()));
		}
		return column.getWholeColumnName();
	}
}
