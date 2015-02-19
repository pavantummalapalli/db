package edu.buffalo.cse562;

import java.io.File;

import edu.buffalo.cse562.utils.TableUtils;

public class Main {
	public static void main(String[] args) {
		if (args.length < 3) {
			printUsage();
			return;
		}
		
		String dataDir = "";
		String sqlFiles[] = new String[args.length - 2];
		int index = 0;
		boolean flag = true;
		for (int i = 0; i < args.length; i++) {
			if (flag && args[i].equals("--data")) {
				dataDir = args[i + 1];
				i++;	
				flag = false;
			} else {
				sqlFiles[index++] = args[i];
			}
		}
		TableUtils.setDataDir(dataDir);
		TableUtils.setTempDataDir(dataDir + File.separator + "temp");
		createTempFolder();
		new StatementReader().readSqlFile(dataDir, sqlFiles);
		//removeTempFolder();
	}

	private static void createTempFolder() {
		new File(TableUtils.getTempDataDir()).mkdir();
	}

	private static void removeTempFolder() {
		File file = new File(TableUtils.getTempDataDir());
		if(file.exists()) {
			File[] files = file.listFiles();
			for(File f:files) {
				f.delete();
			}
		}
		file.delete();
	}

	private static void printUsage() {
		System.out.println("Enter data directory.\n Usage --data Data_Dir SQLFile1.sql [SQLFile2.sql] [SQLFile3.sql]");
	}
}
