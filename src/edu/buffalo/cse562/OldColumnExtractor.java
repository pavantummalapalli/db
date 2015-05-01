package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

public class OldColumnExtractor extends Eval {

	private Set<String> columnNames = new HashSet<>();

	@Override
	public LeafValue eval(Column paramColumn) throws SQLException {
		columnNames.add(paramColumn.getTable().getName() + "." + paramColumn.getColumnName());
		return null;
	}

	public void evalExpressionList(List list) {
		if (list == null)
			return;
		try {
			for (Expression exp : (List<Expression>) list)
				eval(exp);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public LeafValue eval(Function paramFunction) {
		// TODO Auto-generated method stub
		try {
			if (paramFunction.getParameters() == null)
				return null;
			evalExpressionList(paramFunction.getParameters().getExpressions());
			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Set<String> getColumns() {
		return columnNames;
	}

}
