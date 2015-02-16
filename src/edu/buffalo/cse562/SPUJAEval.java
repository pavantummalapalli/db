package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;

public class SPUJAEval {

	private String tableName;
	private List<Table> joins;
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setJoins(List<Table> joins) {
		this.joins = joins;
	}
	
	public List<Table> getJoins() {
		return joins;
	}
	
	public CreateTable eval(){
		for(Join join:joins)
			
		return new CreateTable();
	}
}
