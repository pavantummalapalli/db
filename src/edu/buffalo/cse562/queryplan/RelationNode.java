package edu.buffalo.cse562.queryplan;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.ExtendedCreateTable;
import edu.buffalo.cse562.datasource.DataSource;

public class RelationNode implements Node {
	
	private Node parentNode;
	private String tableName;
	private String aliasName;
	private DataSource file;
	private CreateTable table;
	private Expression expression;
	
	public RelationNode(){}
	
	public RelationNode(String tableName,String aliasName,DataSource file,CreateTable table){
		this.tableName = tableName.toUpperCase();
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
	
	public void setFile(DataSource file) {
		this.file = file;
	}
	
	public DataSource getFile() {
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

	@Override
	public Node getParentNode() {
		return parentNode;
	}

	@Override
	public void setParentNode(Node parentNode) {
		this.parentNode=parentNode;
	}
	
	public void addExpression(Expression exp){
		if(expression!=null){
			Expression exp1 = new AndExpression(expression, exp);
			expression=exp1;
		}
		else
			expression=exp;
	}
	
	public Expression getExpression(){
		return expression;
	}
	
	@Override
	public String toString() {
		if(getAliasName()!=null)
		return getTableName() + " as " + getAliasName();
		else
			return getTableName() ;
	}
}
