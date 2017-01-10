/*
 * MAE - Multi-purpose Annotation Environment
 *
 * Copyright Keigh Rim (krim@brandeis.edu)
 * Department of Computer Science, Brandeis University
 * Original program by Amber Stubbs (astubbs@cs.brandeis.edu)
 *
 * MAE is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, @see <a href="http://www.gnu.org/licenses">http://www.gnu.org/licenses</a>.
 *
 * For feedback, reporting bugs, use the project on Github
 * @see <a href="https://github.com/keighrim/mae-annotation">https://github.com/keighrim/mae-annotation</a>.
 */

package edu.brandeis.cs.nlp.mae.database;


import java.io.File;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.j256.ormlite.jdbc.JdbcConnectionSource;

import edu.brandeis.cs.nlp.mae.model.Task;

public class LocalSqliteDriverImpl extends AbstractDriverImpl {

    // not static for multi file support: needs to instantiate many Drivers
    private Logger logger;

    static final String JDBC_DRIVER = "jdbc:sqlite:";

    private String SQLITE_FILENAME;

    public LocalSqliteDriverImpl(String sqlite_filename) throws MaeDBException {
        SQLITE_FILENAME = sqlite_filename;
        logger = LoggerFactory.getLogger(this.getClass().getName() + SQLITE_FILENAME);
        try {
            cs = new JdbcConnectionSource(JDBC_DRIVER + SQLITE_FILENAME);
            idHandler = new IdHandler();
            this.setupDatabase(cs);
            // put a placeholder for task metadata in DB
            workingTask = new Task(SQLITE_FILENAME);
            taskDao.create(workingTask);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
        logger.info("New JDBC SQLite Driver is initialized, using a local file: " + SQLITE_FILENAME);
        workChanged = false;

    }

 
    @Override
    public String getDBSourceName() {
        return SQLITE_FILENAME;
    }

    /**
     * Shut down data source connection and delete all table from DB.
     */
    @Override
    public void destroy() throws MaeDBException {
        if (cs != null){
            dropAllTables(cs);
            try {
                cs.close();
            } catch (SQLException e) {
                throw catchSQLException(e);
            }
            logger.info("closing JDBC datasource and deleting DB file: " + SQLITE_FILENAME);
            File dbFile = new File(SQLITE_FILENAME);
            if (dbFile.delete()) {
                logger.info("driver is completely destroyed");
            } else {
                logger.error("DB file is not deleted: " + SQLITE_FILENAME);

            }
        }
    }

}

