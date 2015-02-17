package edu.buffalo.cse562.utils;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public final class TableUtils {
	
	private static Map <String, CreateTable> tableSchemaMap = new HashMap <>();

	public static Map<String, CreateTable> getTableSchemaMap() {
		return tableSchemaMap;
	}

	public static void setTableSchemaMap(Map<String, CreateTable> tableSchemaMap) {
		TableUtils.tableSchemaMap = tableSchemaMap;
	}

	private TableUtils() {
		
	}
}