package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ExtendedCreateTable extends CreateTable {
	
	private List<ColumnDefinition> cds;
	private CreateTable table;
	private String alias;
	
	public ExtendedCreateTable(CreateTable table,String alias){
		this.table = table;
		setAlias(alias);
	}

	public void setAlias(String alias) {
		this.alias = alias;
		copyColumnDefinitions(alias);
	}

	private void copyColumnDefinitions(String alias) {
		cds= new ArrayList<>();
		for(ColumnDefinition cd : (List<ColumnDefinition>) table.getColumnDefinitions())
			cds.add(new ExtendedColumnDefinition(cd, alias));
	}
	
	@Override
	public List getColumnDefinitions() {
		// TODO Auto-generated method stub
		return cds;
	}
	
	@Override
	public void setColumnDefinitions(List list) {
		// TODO Auto-generated method stub
		table.setColumnDefinitions(list);
		cds = new ArrayList<>();
		for (ColumnDefinition cd : (List<ColumnDefinition>) table.getColumnDefinitions())
			cds.add(new ExtendedColumnDefinition(cd, alias));
	}
	
	@Override
	public List getIndexes() {
		// TODO Auto-generated method stub
		return table.getIndexes();
	}
	@Override
	public void setIndexes(List list) {
		// TODO Auto-generated method stub
		table.setIndexes(list);
	}
	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return table.getTable();
	}
	@Override
	public void setTable(Table table) {
		// TODO Auto-generated method stub
		this.table.setTable(table);
	}
	@Override
	public List getTableOptionsStrings() {
		// TODO Auto-generated method stub
		return table.getTableOptionsStrings();
	}
	@Override
	public void setTableOptionsStrings(List list) {
		// TODO Auto-generated method stub
		table.setTableOptionsStrings(list);
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return table.toString();
	}
}
