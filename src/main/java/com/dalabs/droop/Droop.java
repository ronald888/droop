package com.dalabs.droop;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * Created by ronaldm on 12/30/2016.
 */
public class Droop {
    static final Logger LOG = LoggerFactory.getLogger(Droop.class);

    static final String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    static final String DB_URL = "jdbc:drill:zk=maprdemo:5181/drill/demo_mapr_com-drillbits";

    static final String USER = "mapr";
    static final String PASS = "mapr";


    public static void main(String[] args) throws IOException, Exception {
        final String message = "USAGE:\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n";
        //Preconditions.checkArgument(args.length > 1, message);

        switch (args[0]) {
            case "producer":
                break;
            case "consumer":
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }

        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

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
            stmt.close();

            conn.close();
        } catch(SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null)
                    stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null)
                    conn.close();
            } catch(SQLException se) {
                se.printStackTrace();
            }
        }

        System.exit(0);
    }
}
