package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class SelectQueryEvaluator implements SelectItemVisitor, FromItemVisitor, ExpressionVisitor,SelectVisitor{

	private String dataDir;
	
	public SelectQueryEvaluator(String dataDir) {
		this.dataDir=dataDir;
	}
	
	@Override
	public void visit(NullValue nullvalue) {
		System.out.println("Null Value");
	}

	@Override
	public void visit(Function function) {
		// TODO Auto-generated method stub
		System.out.println("Function");
	}

	@Override
	public void visit(InverseExpression inverseexpression) {
		// TODO Auto-generated method stub
		System.out.println("Inverse expression");
	}

	@Override
	public void visit(JdbcParameter jdbcparameter) {
		// TODO Auto-generated method stub
		System.out.println("jdbc parameter");
	}

	@Override
	public void visit(DoubleValue doublevalue) {
		// TODO Auto-generated method stub
		System.out.println("Double value");
	}

	@Override
	public void visit(LongValue longvalue) {
		// TODO Auto-generated method stub
		System.out.println("Long Value");
	}

	@Override
	public void visit(DateValue datevalue) {
		// TODO Auto-generated method stub
		System.out.println("Date Value");
	}

	@Override
	public void visit(TimeValue timevalue) {
		// TODO Auto-generated method stub
		System.out.println("Time Value");
	}

	@Override
	public void visit(TimestampValue timestampvalue) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		// TODO Auto-generated method stub
		System.out.println("Paranthesis");
		
	}

	@Override
	public void visit(StringValue stringvalue) {
		// TODO Auto-generated method stub
		System.out.println("stringvalue");
	}

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		System.out.println("Addition");
		visitBinaryExp(addition);
		
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		System.out.println("Division");
		visitBinaryExp(division);
	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		System.out.println("multiplication");
		visitBinaryExp(multiplication);
	}

	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
		System.out.println("Subtraction");
		visitBinaryExp(subtraction);
	}

	@Override
	public void visit(AndExpression andexpression) {
		// TODO Auto-generated method stub
		System.out.println("andexpression");
		visitBinaryExp(andexpression);
	}

	@Override
	public void visit(OrExpression orexpression) {
		// TODO Auto-generated method stub
		System.out.println("orexpression");
		visitBinaryExp(orexpression);
	}

	@Override
	public void visit(Between between) {
		// TODO Auto-generated method stub
		System.out.println("between");
	}

	@Override
	public void visit(EqualsTo equalsto) {
		// TODO Auto-generated method stub
		System.out.println("EqualsTo");
		visitBinaryExp(equalsto);
	}

	@Override
	public void visit(GreaterThan greaterthan) {
		// TODO Auto-generated method stub
		System.out.println("greaterthan");
		visitBinaryExp(greaterthan);
	}

	@Override
	public void visit(GreaterThanEquals greaterthanequals) {
		// TODO Auto-generated method stub
		System.out.println("greaterthanequals");
		visitBinaryExp(greaterthanequals);
	}

	@Override
	public void visit(InExpression inexpression) {
		// TODO Auto-generated method stub
		System.out.println("inexpression");
	}

	@Override
	public void visit(IsNullExpression isnullexpression) {
		// TODO Auto-generated method stub
		System.out.println("isnullexpression");
	}

	@Override
	public void visit(LikeExpression likeexpression) {
		// TODO Auto-generated method stub
		System.out.println("likeexpression");
		visitBinaryExp(likeexpression);
	}

	@Override
	public void visit(MinorThan minorthan) {
		// TODO Auto-generated method stub
		System.out.println("minorthan");
		visitBinaryExp(minorthan);
	}

	@Override
	public void visit(MinorThanEquals minorthanequals) {
		// TODO Auto-generated method stub
		System.out.println("minorthanequals");
		visitBinaryExp(minorthanequals);
	}

	@Override
	public void visit(NotEqualsTo notequalsto) {
		// TODO Auto-generated method stub
		System.out.println("stringvalue");
		visitBinaryExp(notequalsto);
	}

	@Override
	public void visit(Column column) {
		// TODO Auto-generated method stub
		System.out.println("column");
	}

	@Override
	public void visit(CaseExpression caseexpression) {
		// TODO Auto-generated method stub
		System.out.println("caseexpression");
	}

	@Override
	public void visit(WhenClause whenclause) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(ExistsExpression existsexpression) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(AllComparisonExpression allcomparisonexpression) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(AnyComparisonExpression anycomparisonexpression) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		System.out.println("stringvalue");
	}

	@Override
	public void visit(BitwiseAnd bitwiseand) {
		// TODO Auto-generated method stub
		System.out.println("stringvalue");
	}

	@Override
	public void visit(BitwiseOr bitwiseor) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(BitwiseXor bitwisexor) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(Table table) {
		table.getName();
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(AllColumns allcolumns) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(AllTableColumns alltablecolumns) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(SelectExpressionItem selectexpressionitem) {
		// TODO Auto-generated method stub
		System.out.println("Null Value");
	}

	@Override
	public void visit(PlainSelect arg0) {
		arg0.getFromItem().accept(this);
		Expression whereExp = arg0.getWhere();
		if(whereExp != null)
			whereExp.accept(this);
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
		
	}
	
	private void visitBinaryExp(BinaryExpression binaryExp) {
		binaryExp.getLeftExpression();
		binaryExp.getRightExpression();		
	}
}
