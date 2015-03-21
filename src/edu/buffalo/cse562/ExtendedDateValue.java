package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.DateValue;


public class ExtendedDateValue extends DateValue{

	
	public ExtendedDateValue(String value) {
			super(value);
	}
	
	@Override
	public long toLong() throws InvalidLeaf {
		return getValue().getTime();
	}
}
