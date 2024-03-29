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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.utils.TableUtils;

public class ExpressionEvaluator extends Eval {
	
	private LeafValue[] colVals;
	private CreateTable table;
	private HashMap<String, Integer> columnMapping = new HashMap <String, Integer> ();
	private HashMap<GroupBy,Object> calculatedData = new HashMap <>();
	private HashMap <GroupBy, Average> tempAverageMap = new HashMap <>();
	private GroupBy groupByKey;
	private boolean aggregateModeOn;
	private List<Column> groupByList;
	//private HashMap<Column,String> columnWholeNameMap = new HashMap<Column, String>();
		
	private class Average {
		Double value;
		int count;
	}
	
	public void setAggregateModeOn(boolean aggregateModeOn) {
		this.aggregateModeOn = aggregateModeOn;
	}
	
	public boolean isAggregateModeOn() {
		return aggregateModeOn;
	}
	
	public ExpressionEvaluator(CreateTable table){
		if(table==null)
			return;
		this.table = table;
		List<ColumnDefinition> colDefns = table.getColumnDefinitions();
		Iterator<ColumnDefinition> iterator = colDefns.iterator();
		int index = 0;
		while(iterator.hasNext()) {
			ColumnDefinition cd = iterator.next();
			columnMapping.put(cd.getColumnName().toUpperCase(), index++);
		}
	}
	
	@Override
	public LeafValue eval(Function function) throws SQLException {
		try{
			String functionName =function.getName(); 
			if (functionName.equals("SUM")) {
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				GroupBy key=groupByKey;
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
			} else if (functionName.equals("AVG")) {
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				GroupBy key=groupByKey;
								
				Object value= calculatedData.get(key);
				Average avg = tempAverageMap.get(key);
				
				if(evaluatedValue instanceof LongValue){
					if(value==null){
						avg = new Average();
						avg.value =(double)evaluatedValue.toLong();
						avg.count=1;
					}
					else{
						avg.value  = (((Double) avg.value * avg.count + evaluatedValue.toLong()))/ ++avg.count;
					}
					tempAverageMap.put(key, avg);
					calculatedData.put(key, avg.value);					
				}
				else if(evaluatedValue instanceof DoubleValue){
					if(value==null){
						avg = new Average();
						avg.value = evaluatedValue.toDouble();
						avg.count=1;
					}
					else{
						avg.value  = (((Double)avg.value ) * avg.count + evaluatedValue.toDouble())/++avg.count;
					}
					tempAverageMap.put(key, avg);
					calculatedData.put(key, avg.value);
				}
				else {
					value=0;
				}
			}
 else if (functionName.equals("MIN") || functionName.equals("MAX")) {
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				GroupBy key=groupByKey;
				Object value= calculatedData.get(key);
				if(evaluatedValue instanceof LongValue){
					if(value==null){
						value = evaluatedValue.toLong();
					}
					else{
						if (functionName.equals("MIN"))
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
						if (functionName.equals("MIN"))
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
						if (functionName.equals("MIN"))
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
						if (functionName.equals("MIN"))
							value = ((DateValue) value).getValue().compareTo(((DateValue) evaluatedValue).getValue())<=0?((DateValue) value).getValue():((DateValue) evaluatedValue).getValue();
						else
							value = ((DateValue) value).getValue().compareTo(((DateValue) evaluatedValue).getValue())>=0?((DateValue) value).getValue():((DateValue) evaluatedValue).getValue();
					}
					calculatedData.put(key,value);
				}
			}
 else if (functionName.equals("COUNT")) {
				GroupBy key=groupByKey;
				Object value= calculatedData.get(key);
				if(value==null)
					value=1;
				else
					value= (int)value + 1;
				calculatedData.put(key,value);
			} else if (functionName.equals("DATE")) {
				Expression dateValueParam = (Expression) function.getParameters().getExpressions().get(0);
				String dateStr = eval(dateValueParam).toString();
				return TableUtils.getPooledDateValue(dateStr);
			}
		}
		catch(InvalidLeaf e){
			throw new RuntimeException(e);
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
	
	public LeafValue evaluateExpression(Expression exp,LeafValue []colVals,List<Column> groupByList) throws SQLException{
		this.colVals=colVals;
		this.groupByList = groupByList;
		return super.eval(exp);
	}
	
	public LeafValue evaluateExpression(Expression exp,LeafValue []colVals,List<Column> groupByList,GroupBy groupByKey) throws SQLException{
		this.colVals=colVals;
		this.groupByList = groupByList;
		this.groupByKey=groupByKey;
		return super.eval(exp);
	}
	
	public HashMap<GroupBy, Object> getCalculatedData() {
		return calculatedData;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		String value=arg0.getWholeColumnName();
//		if((value =columnWholeNameMap.get(arg0))==null){
//			value = arg0.getWholeColumnName();
//			//Assuming collisions will not occur since the number of values are very very less
//			columnWholeNameMap.put(arg0, value);
//		}
		int index = columnMapping.get(value); 
		return colVals[index];
	}
	
	public GroupBy getGroupByValueKey() throws SQLException{
		return groupByKey;
	}
}
