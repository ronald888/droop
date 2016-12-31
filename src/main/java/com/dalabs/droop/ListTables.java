package com.dalabs.droop;

import java.sql.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * Created by ronaldm on 12/31/2016.
 */
public class ListTables {

    public static final Log LOG = LogFactory.getLog(ListTables.class.getName());

    private final String jdbcDriver;
    private final String dbURL;
    private final String user;
    private final String pass;
    private Connection conn = null;
    private Statement stmt = null;

    public ListTables(final String jdbcDriver, final String dbURL, final String user, final String pass) {
        this.jdbcDriver = jdbcDriver;
        this.dbURL = dbURL;
        this.user = user;
        this.pass = pass;
    }


    public int run() throws Exception {
        LOG.debug("Running...");

        try{
            Class.forName(this.jdbcDriver);
            conn = DriverManager.getConnection(this.dbURL,this.user,this.pass);

            stmt = conn.createStatement();
            /* Perform a select on data in the classpath storage plugin. */
            //String sql = "select age, name from `dfs.root`.`/user/mapr/people.parquet`";
            //String sql = "select EMP_ID, EMP_NAME, EMP_SAL from `test.MAPR`.`ACAD`";
            String sql = "SELECT TABLE_NAME " +
                    "FROM INFORMATION_SCHEMA.`TABLES` " +
                    "WHERE TABLE_SCHEMA = 'oracle.MAPR' and TABLE_TYPE = 'TABLE' " +
                    "ORDER BY TABLE_NAME ASC";
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()) {
                String table = rs.getString("TABLE_NAME");
                System.out.println(table);
            }

            rs.close();
        } catch(SQLException se) {
            //Handle errors for JDBC
            LOG.error("Error with JDBC "
                    + this.jdbcDriver + ":" + StringUtils.stringifyException(se));
            return 1;
        } catch(Exception e) {
            //Handle errors for Class.forName
            LOG.error("Error with Class "
            + this.jdbcDriver + ":" + StringUtils.stringifyException(e));
            return 1;
        } finally {
            try{
                if(stmt!=null) {
                    stmt.close();
                }
            } catch(SQLException se2) {
                LOG.error("Error with closing SQL :" + StringUtils.stringifyException((se2)));
                return 1;
            }
            try {
                if(conn!=null) {
                    conn.close();
                }
            } catch(SQLException se) {
                LOG.error("Error with closing connection :" + StringUtils.stringifyException((se)));
                return 1;
            }
        }

        return 0;
    }
}
