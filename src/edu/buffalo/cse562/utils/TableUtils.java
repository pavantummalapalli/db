package edu.buffalo.cse562.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
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
	private static String tempDataDir;

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
	
	public  static File getAssociatedTableFile(String tableName){
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
