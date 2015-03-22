package edu.buffalo.cse562.fileoperations.sort;

import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.utils.TableUtils;

public class LeafValueConverter implements Convertor<LeafValue[]> {

	private List<ColumnDefinition> columnDefList;
	

	public LeafValueConverter(List<ColumnDefinition> columnDefList) {
		this.columnDefList = columnDefList;
	}

	@Override
	public LeafValue[] parseFromString(String data) {
		LeafValue[] leafValue = new LeafValue[columnDefList.size()];
		String[] colVals = TableUtils.pattern.split(data);
		
		for (int i = 0; i < columnDefList.size(); i++) {
			ColDataType dataType = columnDefList.get(i).getColDataType();
			String str = dataType.getDataType();
			if (str.equals("INT") || str.equals("int"))
				leafValue[i] = new LongValue(colVals[i]);
			else if (str.equals("DATE") || str.equals("date")) {
				String dateValueStr="'"+ colVals[i] + "'";
				leafValue[i] = TableUtils.getPooledDateValue(dateValueStr);
			} else if (str.equals("STRING") || str.contains("CHAR") || str.equals("string")  || str.contains("char"))
				leafValue[i] = new StringValue(" " + colVals[i] + " ");
			else if (str.equals("DECIMAL") || str.equals("DOUBLE") || str.equals("double") || str.equals("decimal"))
				leafValue[i] = new DoubleValue(colVals[i]);
		}
		return leafValue;
	}

	@Override
	public String convertToString(LeafValue[] data) {
		return TableUtils.convertToString(data);
	}
}
