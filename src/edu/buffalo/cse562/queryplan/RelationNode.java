package edu.buffalo.cse562.queryplan;

public class RelationNode implements Node {
	
	private String tableName;
	private String aliasName;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName){
		
	}
	
	@Override
	public RelationNode eval() {
		// TODO Auto-generated method stub
		return null;
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
