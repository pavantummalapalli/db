/*package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.Iterator;

import edu.buffalo.cse562.queryplan.QueryDomain;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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
			throw new RuntimeException("SQLEXception in ExpressionResolver resolveExpression ", e);
		}
		return exp;
	}
	
	@Override
	public LeafValue eval(Function function) throws SQLException {
		resolveFunction(function);
		return null;
	}
	
	public Function resolveFunction(Function function) {
		Iterator<Expression> expressionIterator = function.getParameters().getExpressions().iterator();
		while(expressionIterator.hasNext())
			resolveExpression(expressionIterator.next());
		return function;
	}
	
	@Override
	public LeafValue eval(Column paramColumn) throws SQLException {
		queryDomain.resolveColumn(paramColumn);
		return null;
	}
}
*/