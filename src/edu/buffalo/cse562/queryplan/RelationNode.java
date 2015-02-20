package edu.buffalo.cse562.queryplan;

import java.io.File;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.ExtendedCreateTable;

public class RelationNode implements Node {
	
	private String tableName;
	private String aliasName;
	private File file;
	private CreateTable table;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName,File file,CreateTable table){
		this.tableName = tableName;
		this.file=file;
//		if(aliasName!=null && !aliasName.isEmpty())
//			this.aliasName = aliasName;
//		else
//			this.aliasName=tableName;
//		this.file=file;
		this.table=new ExtendedCreateTable(table,aliasName);
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
		return this;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setAliasName(String aliasName) {
		((ExtendedCreateTable) table).setAlias(aliasName);
		this.aliasName = aliasName;
	}
	
	public String getAliasName() {
		return aliasName;
	}

	@Override
	public CreateTable evalSchema() {
		return table;
	}
}
