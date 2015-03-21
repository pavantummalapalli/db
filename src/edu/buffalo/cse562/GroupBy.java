package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

public class GroupBy {
		
		private LeafValue[] leafValues;
		private String keyString;
		
		public LeafValue[] getLeafValue() {
			return leafValues;
		}
		
		public GroupBy(LeafValue[] values) {
			this.leafValues = values;
			StringBuilder key = new StringBuilder("");
			for(int i=0;i<leafValues.length;i++){
				key.append(convertLeafValueToString(leafValues[i])+"|");
			}
			keyString= key.substring(0,key.length()-1);
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof GroupBy){
				return toString().equals(obj.toString());
			}
			return false;
		}
		
		@Override
		public String toString() {
			return keyString;
		}
		
		public String convertLeafValueToString(LeafValue leafValue) {
			if (leafValue instanceof DoubleValue)
				return String.valueOf(((DoubleValue) leafValue).getValue());
			else if (leafValue instanceof LongValue)
				return String.valueOf(((LongValue) leafValue).getValue());
			else if (leafValue instanceof StringValue)
				return ((StringValue) leafValue).getValue();
			else if (leafValue instanceof DateValue)
				return String.valueOf(((DateValue) leafValue).toString());
			return "";
		}
		
		@Override
		public int hashCode() {
			return keyString.hashCode();
		}
	}