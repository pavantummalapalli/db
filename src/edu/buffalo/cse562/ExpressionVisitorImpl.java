package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.queryplan.QueryDomain;

public class ExpressionVisitorImpl implements ExpressionVisitor {

	
	private QueryDomain queryDomain;

	public ExpressionVisitorImpl(QueryDomain queryDomain) {
		this.queryDomain = queryDomain;
	}
	
	@Override
	public void visit(NullValue paramNullValue) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(Function paramFunction) {
		// TODO Auto-generated method stub
		try{
			if(paramFunction.getParameters()==null)
				return;
			paramFunction.setName(paramFunction.getName().toUpperCase());
			resolveExpressionList(paramFunction.getParameters().getExpressions());
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void resolveExpressionList(List list) {
		if(list==null)
			return;
		try{
		for(Expression exp : (List<Expression>)list)
			exp.accept(this);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void visit(InverseExpression paramInverseExpression) {
		paramInverseExpression.getExpression().accept(this);

	}

	@Override
	public void visit(JdbcParameter paramJdbcParameter) {

	}

	@Override
	public void visit(DoubleValue paramDoubleValue) {
	}

	@Override
	public void visit(LongValue paramLongValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue paramDateValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue paramTimeValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue paramTimestampValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis paramParenthesis) {
		// TODO Auto-generated method stub
		paramParenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue paramStringValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition paramAddition) {
		paramAddition.getLeftExpression().accept(this);
		paramAddition.getRightExpression().accept(this);

	}

	@Override
	public void visit(Division paramDivision) {
		// TODO Auto-generated method stub
		paramDivision.getLeftExpression().accept(this);
		paramDivision.getRightExpression().accept(this);

	}

	@Override
	public void visit(Multiplication paramMultiplication) {
		paramMultiplication.getLeftExpression().accept(this);
		paramMultiplication.getRightExpression().accept(this);
	}

	@Override
	public void visit(Subtraction paramSubtraction) {
		paramSubtraction.getLeftExpression().accept(this);
		paramSubtraction.getRightExpression().accept(this);
	}

	@Override
	public void visit(AndExpression paramAndExpression) {
		paramAndExpression.getLeftExpression().accept(this);
		paramAndExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression paramOrExpression) {
		paramOrExpression.getLeftExpression().accept(this);
		paramOrExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(Between paramBetween) {
		paramBetween.getBetweenExpressionEnd().accept(this);
		paramBetween.getBetweenExpressionStart().accept(this);
		paramBetween.getLeftExpression().accept(this);
	}

	@Override
	public void visit(EqualsTo paramEqualsTo) {
		paramEqualsTo.getLeftExpression().accept(this);
		paramEqualsTo.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThan paramGreaterThan) {
		paramGreaterThan.getLeftExpression().accept(this);
		paramGreaterThan.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThanEquals paramGreaterThanEquals) {
		paramGreaterThanEquals.getLeftExpression().accept(this);
		paramGreaterThanEquals.getRightExpression().accept(this);

	}

	@Override
	public void visit(InExpression paramInExpression) {
		
	}

	@Override
	public void visit(IsNullExpression paramIsNullExpression) {
		paramIsNullExpression.getLeftExpression().accept(this);
	}

	@Override
	public void visit(LikeExpression paramLikeExpression) {
		paramLikeExpression.getLeftExpression().accept(this);
		paramLikeExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(MinorThan paramMinorThan) {
		paramMinorThan.getLeftExpression().accept(this);
		paramMinorThan.getRightExpression().accept(this);
	}

	@Override
	public void visit(MinorThanEquals paramMinorThanEquals) {
		paramMinorThanEquals.getLeftExpression().accept(this);
		paramMinorThanEquals.getRightExpression().accept(this);
	}

	@Override
	public void visit(NotEqualsTo paramNotEqualsTo) {
		paramNotEqualsTo.getLeftExpression().accept(this);
		paramNotEqualsTo.getRightExpression().accept(this);
	}

	@Override
	public void visit(Column paramColumn) {
		queryDomain.resolveColumn(paramColumn);
	}

	@Override
	public void visit(SubSelect paramSubSelect) {
		
	}

	@Override
	public void visit(CaseExpression paramCaseExpression) {
		// TODO Auto-generated method stub
		if(paramCaseExpression.getElseExpression()!=null)
			paramCaseExpression.getElseExpression().accept(this);
		if(paramCaseExpression.getSwitchExpression()!=null)
			paramCaseExpression.getSwitchExpression().accept(this);
		List<WhenClause> list =paramCaseExpression.getWhenClauses(); 
		if(list!=null){
			for(WhenClause clause: list)
				clause.accept(this);
		}
	}

	@Override
	public void visit(WhenClause paramWhenClause) {
		// TODO Auto-generated method stub
		paramWhenClause.getThenExpression().accept(this);
		paramWhenClause.getWhenExpression().accept(this);
	}

	@Override
	public void visit(ExistsExpression paramExistsExpression) {
		paramExistsExpression.getRightExpression().accept(this);
	}

	@Override
	public void visit(AllComparisonExpression paramAllComparisonExpression) {
	}

	@Override
	public void visit(AnyComparisonExpression paramAnyComparisonExpression) {
	}

	@Override
	public void visit(Concat paramConcat) {
		paramConcat.getLeftExpression().accept(this);
		paramConcat.getRightExpression().accept(this);
	}

	@Override
	public void visit(Matches paramMatches) {
		paramMatches.getLeftExpression().accept(this);
		paramMatches.getRightExpression().accept(this);

	}

	@Override
	public void visit(BitwiseAnd paramBitwiseAnd) {
		paramBitwiseAnd.getLeftExpression().accept(this);
		paramBitwiseAnd.getRightExpression().accept(this);
	}

	@Override
	public void visit(BitwiseOr paramBitwiseOr) {
		paramBitwiseOr.getLeftExpression().accept(this);
		paramBitwiseOr.getRightExpression().accept(this);
	}

	@Override
	public void visit(BitwiseXor paramBitwiseXor) {
		paramBitwiseXor.getLeftExpression().accept(this);
		paramBitwiseXor.getRightExpression().accept(this);

	}
}
