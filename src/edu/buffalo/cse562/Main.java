package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.berkelydb.InitializeTableIndexMetaData;
import edu.buffalo.cse562.utils.TableUtils;

public class Main {
	public static void main(String[] args) {
		if (args.length < 3) {
			printUsage();
			return;
		}
		
		String dataDir = "", swapDir = null, dbDir = null;
		List <String> fileList = new ArrayList <>();
		int index = 0;
		boolean flag = true;
		for (int i = 0; i < args.length; i++) {
			if (flag && args[i].equals("--data")) {
				dataDir = args[i + 1];
				i++;	
				flag = false;
			} else if (args[i].equals("--swap")) {
				swapDir = args[i + 1];
				i++;
			} else if (args[i].equals("--load")) {
                TableUtils.isLoadPhase = true;
            } else if (args[i].equals("--db")) {
                dbDir = args[i + 1];
                i++;
            } else {
				fileList.add(args[i]);
			}
		}
		TableUtils.setDataDir(dataDir);
		if (swapDir != null) {
			TableUtils.setTempDataDir(swapDir);
			TableUtils.isSwapOn = true;
		}
        if (dbDir != null) {
            TableUtils.setDbDir(dbDir);
        }
		//createTempFolder();
		new StatementReader().readSqlFile(dataDir, fileList.toArray(new String[fileList.size()]));
	}

	private static void createTempFolder() {
		new File(TableUtils.getTempDataDir()).mkdir();
	}

	private static void printUsage() {
		System.out.println("Enter data directory.\n Usage --data Data_Dir SQLFile1.sql [SQLFile2.sql] [SQLFile3.sql]");
	}
}
