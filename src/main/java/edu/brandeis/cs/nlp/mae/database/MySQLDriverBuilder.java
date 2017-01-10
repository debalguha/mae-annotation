package edu.brandeis.cs.nlp.mae.database;

import java.io.IOException;
import java.util.Properties;

import edu.brandeis.cs.nlp.mae.MaeException;

public class MySQLDriverBuilder {
	public static MaeDriverI buildDriverFromProperties() throws MaeException {
		Properties applicationProperties = new Properties();
    	try {
			applicationProperties.load(MySQLDriverBuilder.class.getClassLoader().getResourceAsStream("application.properties"));
		} catch (IOException e) {
			throw new MaeException(e);
		}
    	return new MySQlDriverImpl(applicationProperties.getProperty("jdbc.url"), applicationProperties.getProperty("jdbc.username"), applicationProperties.getProperty("jdbc.password"));
	}
}
