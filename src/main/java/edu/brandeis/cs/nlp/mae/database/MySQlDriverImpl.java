package edu.brandeis.cs.nlp.mae.database;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.j256.ormlite.jdbc.JdbcConnectionSource;

import edu.brandeis.cs.nlp.mae.model.Task;

public class MySQlDriverImpl extends AbstractDriverImpl {
	private static final Logger logger = LoggerFactory.getLogger(MySQlDriverImpl.class);
	private final String url;

	public MySQlDriverImpl(String url, String userName, String password) throws MaeDBException {
		this.url = url;
		try {
			cs = new JdbcConnectionSource(url, userName, password);
			idHandler = new IdHandler();
			this.setupDatabase(cs);
			// put a placeholder for task metadata in DB
			workingTask = new Task("PLACE_HOLDER");
			taskDao.create(workingTask);
		} catch (SQLException e) {
			throw catchSQLException(e);
		}
		logger.info("New JDBC MySQL Driver is initialized, using url: " + url);
        workChanged = false;
	}

	@Override
	public void destroy() throws MaeDBException {
		if (cs != null) {
			logger.info("closing JDBC datasource with url: " + url);
			try {
				cs.close();
			} catch (SQLException e) {
				throw catchSQLException(e);
			}

		}

	}

	@Override
	public String getDBSourceName() {
		return url;
	}

}
