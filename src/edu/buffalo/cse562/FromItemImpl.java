package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
	private List <Table> tableList = new ArrayList<>();
	
	public FromItemImpl(){
	}
	
	@Override
	public void visit(Table table) {
		File filePath = TableUtils.getAssociatedTableFile(table.getName());
		CreateTable schema =TableUtils.getTableSchemaMap().get(table.getName().toUpperCase());
		if(table.getAlias()==null)
			table.setAlias(table.getName());
		node = new RelationNode(table.getName(),table.getAlias(),filePath,schema);
		tableList.add(table);
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		SelectVisitorImpl selectVistor=new SelectVisitorImpl();
		subselect.getSelectBody().accept(selectVistor);
		ProjectNode tempNode = (ProjectNode)selectVistor.getQueryPlanTreeRoot();
		tempNode.setPreferredAliasName(subselect.getAlias());
		//tableList.add(subselect.getAlias());
		//queryDomain.getQueryDomainTableSchemaMap().put(subselect.getAlias(), tempNode.evalSchema());
		node=tempNode;
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Subjoin not supported");
	}
	
	public List<Table> getTableList() {
		return tableList;
	}
	
	public Node getFromItemNode(){
		return node;
	}
}
