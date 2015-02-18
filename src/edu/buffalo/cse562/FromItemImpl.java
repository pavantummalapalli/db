package edu.buffalo.cse562;

import java.io.File;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.queryplan.Node;
import edu.buffalo.cse562.queryplan.ProjectNode;
import edu.buffalo.cse562.queryplan.RelationNode;
import edu.buffalo.cse562.utils.TableUtils;

public class FromItemImpl implements FromItemVisitor {

	private Node node;

	@Override
	public void visit(Table table) {
		File filePath = new File(TableUtils.getDataDir() + File.separator + table.getName() + ".dat");
		CreateTable schema =TableUtils.getTableSchemaMap().get(table.getName());
		if(table.getAlias()==null)
			table.setAlias(table.getName());
		node = new RelationNode(table.getName(),table.getAlias(),filePath,schema);
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		SelectVisitorImpl selectVistor=new SelectVisitorImpl();
		subselect.getSelectBody().accept(selectVistor);
		ProjectNode tempNode = (ProjectNode)selectVistor.getQueryPlanTreeRoot();
		tempNode.setPreferredAliasName(subselect.getAlias());
		node=tempNode;
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Subjoin not supported");
	}
	
	public Node getFromItemNode(){
		return node;
	}
}
