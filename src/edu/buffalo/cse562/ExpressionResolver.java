package edu.buffalo.cse562;

import java.sql.SQLException;

import edu.buffalo.cse562.queryplan.QueryDomain;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;

public class ExpressionResolver extends Eval {
	
	private QueryDomain queryDomain;
	
	public ExpressionResolver(QueryDomain queryDomain) {
		this.queryDomain=queryDomain;
	}
	
	public Expression resolveExpression(Expression exp){
		try {
			eval(exp);
		} catch (SQLException e) {
			//Ignore the error. Just don't resolve the expression anymore
		}
		return exp;
	}
	
	@Override
	public LeafValue eval(Column paramColumn) throws SQLException {
		queryDomain.resolveColumn(paramColumn);
		return null;
	}
}
