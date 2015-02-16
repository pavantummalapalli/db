package edu.buffalo.cse562;

import java.util.List;
import edu.buffalo.cse562.utils.TableUtils;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SPUJAEval {

	private String tableName;
	private List<CreateTable> joins;
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setJoins(List<CreateTable> joins) {
		this.joins = joins;
	}
	
	public List<CreateTable> getJoins() {
		return joins;
	}
	
	public CreateTable eval(){
		CreateTable table = TableUtils.getTableSchemaMap().get(tableName);
		for(CreateTable join:joins){
			table=evaluate(table, join);
		}
		return new CreateTable();
	}
	
	public CreateTable evaluate(CreateTable table, CreateTable b){
		return new CreateTable();
	}
}
