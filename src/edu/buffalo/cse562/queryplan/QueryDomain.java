package edu.buffalo.cse562.queryplan;

import java.util.Map;

import net.sf.jsqlparser.schema.Column;

public interface QueryDomain {

	public Column resolveColumn(Column column);
	
	public Map<String, String> getColumnTableMap();
	
}
