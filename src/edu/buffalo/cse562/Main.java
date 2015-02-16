package edu.buffalo.cse562;

public class Main {
	public static void main(String[] args) {
		if (args.length < 3) {
			printUsage();
			return;
		}
		
		String dataDir = "";
		String sqlFiles[] = new String[args.length - 2];
		int index = 0;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--data")) {
				dataDir = args[i + 1];
				i++;				
			} else {
				sqlFiles[index++] = args[i];
			}
		}
		new StatementReader().readSqlFile(dataDir, sqlFiles);
	}

	private static void printUsage() {
		System.out.println("Enter data directory.\n Usage --data Data_Dir SQLFile1.sql [SQLFile2.sql] [SQLFile3.sql]");
	}
}
