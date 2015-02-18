package edu.buffalo.cse562.queryplan;

import java.io.File;

import net.sf.jsqlparser.statement.create.table.CreateTable;

public class RelationNode implements Node {
	
	private String tableName;
	private String aliasName;
	private File filePath;
	private CreateTable schema;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName,File filePath,CreateTable schema){
		this.tableName = tableName;
		this.aliasName = aliasName;
		this.filePath=filePath;
		this.schema=schema;
	}
	
	public void setSchema(CreateTable schema) {
		this.schema = schema;
	}
	
	public CreateTable getSchema() {
		return schema;
	}
	
	public void setFilePath(File filePath) {
		this.filePath = filePath;
	}
	
	public File getFilePath() {
		return filePath;
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
