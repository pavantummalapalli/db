package edu.buffalo.cse562.queryplan;

import java.io.File;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.ExtendedCreateTable;

public class RelationNode implements Node {
	
	private String tableName;
	private String aliasName;
	private File filePath;
	private CreateTable schema;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName,File filePath,CreateTable schema){
		this.tableName = tableName;
		if(aliasName!=null && !aliasName.isEmpty())
			this.aliasName = aliasName;
		else
			this.aliasName=tableName;
		this.filePath=filePath;
		this.schema=new ExtendedCreateTable(schema,aliasName);
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
		return this;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setAliasName(String aliasName) {
		((ExtendedCreateTable)schema).setAlias(aliasName);
		this.aliasName = aliasName;
	}
	
	public String getAliasName() {
		return aliasName;
	}
}
