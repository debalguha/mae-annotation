package edu.brandeis.cs.nlp.mae;

import java.io.IOException;
import java.util.Properties;

import edu.brandeis.cs.nlp.mae.database.LocalSqliteDriverImpl;
import edu.brandeis.cs.nlp.mae.database.MaeDriverI;
import edu.brandeis.cs.nlp.mae.database.MySQLDriverBuilder;

public abstract class AbstractDatabaseDrivenTest {
	protected MaeDriverI driver;
	protected void setupDriver() throws MaeException{
		Properties applicationProperties = new Properties();
    	try {
			applicationProperties.load(MySQLDriverBuilder.class.getClassLoader().getResourceAsStream("application.properties"));
		} catch (IOException e) {
			throw new MaeException(e);
		}
    	if(Boolean.valueOf(applicationProperties.getProperty("useSqlite")))
    		driver = new LocalSqliteDriverImpl(MaeStrings.TEST_DB_FILE);
    	else
    		driver = MySQLDriverBuilder.buildDriverFromProperties();
	}
}
