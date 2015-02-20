package edu.buffalo.cse562;

import java.sql.Date;

import net.sf.jsqlparser.expression.DateValue;


public class ExtendedDateValue extends DateValue{

	public ExtendedDateValue(String value) {
		super(value);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public long toLong() throws InvalidLeaf {
		Date date =Date.valueOf(toString());
		return date.getTime();
	}
}
