package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javafx.scene.Group;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.utils.TableUtils;

public class ExpressionEvaluator extends Eval {
	
	private LeafValue[] colVals;
	private CreateTable table;
	private HashMap<String, Integer> columnMapping = new HashMap <String, Integer> ();
	private HashMap<GroupBy,Object> calculatedData = new HashMap <>();
	private HashMap <GroupBy, Average> tempAverageMap = new HashMap <>();
	private List<Column> groupByList;
	private boolean aggregateModeOn;
		
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
			if(function.getName().equalsIgnoreCase("SUM")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				GroupBy key=null;
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
				GroupBy key=null;
								
				if(groupByList!=null && groupByList.size()>0){
					key = getGroupByValueKey();
				}
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
			else if(function.getName().equalsIgnoreCase("MIN") || function.getName().equalsIgnoreCase("MAX")){
				Expression exp =(Expression)function.getParameters().getExpressions().get(0);
				LeafValue evaluatedValue = eval(exp);
				GroupBy key=null;
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
				GroupBy key=null;
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
			else if(function.getName().equalsIgnoreCase("DATE")){
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
	
	public HashMap<GroupBy, Object> getCalculatedData() {
		return calculatedData;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {	
		//String value = arg0.getWholeColumnName().toUpperCase();
		String value = arg0.getWholeColumnName();
		int index = columnMapping.get(value); 
		return colVals[index];
	}
	
	public GroupBy getGroupByValueKey() throws SQLException{
		LeafValue[] leafValues = new LeafValue[groupByList.size()];
		int i=0;
		for(Column groupByColumn : groupByList) {
			//Column temp = convertStringToColumn(groupByColumn.toUpperCase());
			//Column temp = convertStringToColumn(groupByColumn);
			leafValues[i++] = eval(groupByColumn);
		}	
		return new GroupBy(leafValues);
	}
	
}
