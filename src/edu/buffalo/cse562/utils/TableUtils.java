package edu.buffalo.cse562.utils;

import java.util.HashMap;
import java.util.Map;

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
}
