package edu.buffalo.cse562.queryplan;

import java.io.File;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class RelationNode implements Node {
	
	private String tableName;
	private String aliasName;
	private File file;
	private CreateTable table;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName,File file,CreateTable table){
		this.tableName = tableName;
		this.aliasName = aliasName;
		this.file=file;
		this.table=table;
	}
	
	public void setTable(CreateTable table) {
		this.table = table;
	}
	
	public CreateTable getTable() {
		return table;
	}
	
	public void setFile(File file) {
		this.file = file;
	}
	
	public File getFile() {
		return file;
	}
	
	@Override
	public RelationNode eval() {
		// TODO Auto-generated method stub
		return this;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}
	
	public String getAliasName() {
		return aliasName;
	}
}
