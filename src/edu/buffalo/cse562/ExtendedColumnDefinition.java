package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class ExtendedColumnDefinition extends ColumnDefinition {
	
	private ColumnDefinition cd;
	private String alias;
	private String DOT_STR=".";
	
	public ExtendedColumnDefinition(ColumnDefinition cd,String alias){
		this.cd=cd;
		this.alias=alias;
	}
	
	@Override
	public void setColDataType(ColDataType type) {
		// TODO Auto-generated method stub
		cd.setColDataType(type);
	}
	@Override
	public ColDataType getColDataType() {
		// TODO Auto-generated method stub
		return cd.getColDataType();
	}
	
	@Override
	public List getColumnSpecStrings() {
		// TODO Auto-generated method stub
		return cd.getColumnSpecStrings();
	}
	
	@Override
	public void setColumnSpecStrings(List list) {
		// TODO Auto-generated method stub
		cd.setColumnSpecStrings(list);
	}
	
	@Override
	public void setColumnName(String string) {
		// TODO Auto-generated method stub
		cd.setColumnName(string);
	}
	
	@Override
	public String getColumnName() {
		// TODO Auto-generated method stub
		if(alias!=null){
			return alias.toUpperCase()+DOT_STR+getPlainColumnName(cd);
		}else
			return cd.getColumnName().toUpperCase();
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		if(alias!=null){
			return alias.toUpperCase()+DOT_STR+getPlainColumnName(cd);
		}
		return cd.getColumnName().toUpperCase();
	}
	
	public String getPlainColumnName(ColumnDefinition cd){
		if(cd instanceof ExtendedColumnDefinition){
			return getPlainColumnName(((ExtendedColumnDefinition) cd).getInnerColumnDefinitionInstance());
		}
		else
			return cd.getColumnName().toUpperCase();
	}
	
	private ColumnDefinition getInnerColumnDefinitionInstance(){
		return cd;
	}
}
