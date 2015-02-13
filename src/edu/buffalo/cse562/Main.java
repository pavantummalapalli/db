package edu.buffalo.cse562;

public class Main {
	public static void main(String[] args) {
		System.out.print("We, the members of our team, agree that we will not submit any code that we have not written ourselves, share our code with anyone outside of our group, or use code that we have not written ourselves as a reference.");
		if(args.length==0){
			printUsage();
			return;
		}
		int i=0;
		for(;i<args.length ;i++)
			if(args[i].equalsIgnoreCase("--data"))
				break;
		if(i+2>=args.length){
			printUsage();
			return;
		}
		String dataDir = args[++i];
		String sqlFiles[]= new String[args.length-i-1];
		int count=0;
		while(++i<args.length){
			sqlFiles[count++]=args[i];
		}
		new StatementReader().readSqlFile(dataDir,sqlFiles);
	}
	
	private static void printUsage(){
		System.out.println("Enter data directory.\n Usage --data Data_Dir SQLFile1.sql [SQLFile2.sql] [SQLFile3.sql]");
	}
}
