package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ExpressionEvaluator extends Eval {
	
	private String[] colVals;
	private CreateTable table;
	private HashMap<String, Integer> columnMapping = new HashMap <String, Integer> ();
	private HashMap<String,Object> calculatedData = new HashMap <String, Object>();
	private List<String> groupByList;
	private Average avg = new Average();
	
	private class Average{
		Double value;
		int count;
	}
	
	public ExpressionEvaluator(CreateTable table){
		this.table = table;
		List<ColumnDefinition> colDefns = table.getColumnDefinitions();
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while(iterator.hasNext()) {
			ColumnDefinition cd = iterator.next();
			columnMapping.put(cd.getColumnName(), index++);
		}
	}
	
	public String getGroupByValueKey() throws SQLException{
		StringBuffer key = new StringBuffer("");
		for(String groupByColumn : groupByList)
			key.append(getLeafValue(eval(convertStringToColumn(groupByColumn)))+",");
		return key.substring(0,key.length()-1);
	}
	
	private Column convertStringToColumn(String columnStr){
		String [] splitColumnNames = columnStr.split("\\.");
		Column column = new Column();
		if(splitColumnNames.length==2){
			column.setColumnName(splitColumnNames[1]);
			column.setTable(new Table());
			column.getTable().setName(splitColumnNames[0]);
		}
		else{
			column.setColumnName(splitColumnNames[0]);
		}
		return column;
	}
	
	@Override
	public LeafValue eval(Function function) throws SQLException {
		try{
			if(function.getName().equalsIgnoreCase("SUM")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				String key=null;
				if(groupByList!=null && groupByList.size()>0){
					key = getGroupByValueKey();
				}
				Object value= calculatedData.get(key);
				if(evaluatedValue instanceof LongValue){
					if(value==null){
						value = evaluatedValue.toLong();
					}
					else{
						value = ((Long)value) + evaluatedValue.toLong();
					}
					calculatedData.put(key,value);
				}
				else if(evaluatedValue instanceof DoubleValue){
					if(value==null){
						value = evaluatedValue.toDouble();
					}
					else{
						value = ((Double)value) + evaluatedValue.toDouble();
					}
					calculatedData.put(key,value);
				}
				else {
					value=0;
				}
			}
			else if(function.getName().equalsIgnoreCase("AVG")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				String key=null;
				if(groupByList!=null && groupByList.size()>0){
					key = getGroupByValueKey();
				}
				Object value= calculatedData.get(key);
				
				if(evaluatedValue instanceof LongValue){
					if(value==null){
						avg.value =(double)evaluatedValue.toLong();
						avg.count=1;
					}
					else{
						avg.value  = (((Double)avg.value ) * avg.count + evaluatedValue.toLong())/++avg.count;
					}
					value=avg;
					calculatedData.put(key,avg.value);
				}
				else if(evaluatedValue instanceof DoubleValue){
					if(value==null){
						value = evaluatedValue.toDouble();
					}
					else{
						avg.value  = (((Double)avg.value ) * avg.count + evaluatedValue.toDouble())/++avg.count;
					}
					calculatedData.put(key,value);
				}
				else {
					value=0;
				}
			}
			else if(function.getName().equalsIgnoreCase("MIN") || function.getName().equalsIgnoreCase("MAX")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				String key=null;
				if(groupByList!=null && groupByList.size()>0){
					key = getGroupByValueKey();
				}
				Object value= calculatedData.get(key);
				if(evaluatedValue instanceof LongValue){
					if(value==null){
						value = evaluatedValue.toLong();
					}
					else{
						if(function.getName().equalsIgnoreCase("MIN"))
							value = Math.min((Long)value, evaluatedValue.toLong());
						else
							value = Math.max((Long)value, evaluatedValue.toLong());
					}
					calculatedData.put(key,value);
				}
				else if(evaluatedValue instanceof DoubleValue){
					if(value==null){
						value = evaluatedValue.toDouble();
					}
					else{
						if(function.getName().equalsIgnoreCase("MIN"))
							value = Math.min(((Double)value),evaluatedValue.toDouble());
						else
							value = Math.max(((Double)value),evaluatedValue.toDouble());
					}
					calculatedData.put(key,value);
				}
				else if(evaluatedValue instanceof StringValue){
					if(value==null){
						value = ((StringValue) evaluatedValue).toString();
					}
					else{
						if(function.getName().equalsIgnoreCase("MIN"))
							value = value.toString().compareToIgnoreCase(evaluatedValue.toString())<=0?value.toString():evaluatedValue.toString();
						else
							value = value.toString().compareToIgnoreCase(evaluatedValue.toString())>=0?value.toString():evaluatedValue.toString();
					}
					calculatedData.put(key,value);
				}
				else if(evaluatedValue instanceof DateValue){
					if(value==null){
						value = ((DateValue) evaluatedValue).getValue();
					}
					else{
						if(function.getName().equalsIgnoreCase("MIN"))
							value = ((DateValue) value).getValue().compareTo(((DateValue) evaluatedValue).getValue())<=0?((DateValue) value).getValue():((DateValue) evaluatedValue).getValue();
						else
							value = ((DateValue) value).getValue().compareTo(((DateValue) evaluatedValue).getValue())>=0?((DateValue) value).getValue():((DateValue) evaluatedValue).getValue();
					}
					calculatedData.put(key,value);
				}
			}
			else if(function.getName().equalsIgnoreCase("COUNT")){
				String key=null;
				if(groupByList!=null && groupByList.size()>0){
					key = getGroupByValueKey();
				}
				Object value= calculatedData.get(key);
				if(value==null)
					value=1;
				else
					value= (int)value + 1;
				calculatedData.put(key,value);
			}
			return new LeafValue() {
				
				@Override
				public long toLong() throws InvalidLeaf {
					// TODO Auto-generated method stub
					return 0;
				}
				
				@Override
				public double toDouble() throws InvalidLeaf {
					// TODO Auto-generated method stub
					return 0;
				}
			};
		}
		catch(InvalidLeaf e){
			e.printStackTrace();
		}
		return super.eval(function);
	}
	
	public LeafValue evaluateExpression(Expression exp,String []colVals,List<String> groupByList) throws SQLException{
		this.colVals=colVals;
		this.groupByList = groupByList;
		return super.eval(exp);
	}
	
	public HashMap<String, Object> getCalculatedData() {
		return calculatedData;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {			
		int index = columnMapping.get(arg0.getWholeColumnName());
		List<ColumnDefinition> colDefns = table.getColumnDefinitions();
		ColDataType dataType = colDefns.get(index).getColDataType();
		if(dataType.getDataType().equalsIgnoreCase("int"))
			return new LongValue(colVals[index]);
		else if(dataType.getDataType().equalsIgnoreCase("date"))
			return new DateValue(colVals[index]);
		else if(dataType.getDataType().equalsIgnoreCase("string"))
			return new StringValue(colVals[index]);
		else if(dataType.getDataType().equalsIgnoreCase("double"))
			return new DoubleValue(colVals[index]);
		return null;
	}
	public String getLeafValue(LeafValue leafValue) {
		if (leafValue instanceof DoubleValue)
			return String.valueOf(((DoubleValue) leafValue).getValue());
		else if (leafValue instanceof LongValue)
			return String.valueOf(((LongValue) leafValue).getValue());
		else if (leafValue instanceof StringValue)
			return ((StringValue) leafValue).getValue();
		else if (leafValue instanceof DateValue)
			return String.valueOf(((DateValue) leafValue).getDate());
		//TODO throw Unsupported 
		return "";
	}
	public String getLeafValue(LongValue leafValue) {
		return String.valueOf(leafValue.getValue());
	}
	public String getLeafValue(StringValue leafValue) {
		return leafValue.getValue();
	}
	public String getLeafValue(DateValue leafValue) {
		return String.valueOf(leafValue.getDate());
	}

}
