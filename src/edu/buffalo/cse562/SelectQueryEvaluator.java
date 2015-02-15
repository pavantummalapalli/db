package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectQueryEvaluator implements SelectItemVisitor, FromItemVisitor, ExpressionVisitor {

	@Override
	public void visit(NullValue nullvalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function function) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression inverseexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter jdbcparameter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue doublevalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue longvalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue datevalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue timevalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue timestampvalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue stringvalue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression andexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrExpression orexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between between) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo equalsto) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThan greaterthan) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThanEquals greaterthanequals) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression inexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression isnullexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression likeexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan minorthan) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThanEquals minorthanequals) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotEqualsTo notequalsto) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column column) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression caseexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause whenclause) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression existsexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression allcomparisonexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression anycomparisonexpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd bitwiseand) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr bitwiseor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor bitwisexor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllColumns allcolumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns alltablecolumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem selectexpressionitem) {
		// TODO Auto-generated method stub
		
	}

}
