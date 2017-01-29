package com.dalabs.droop;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.jdbc.Driver;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ronaldm on 16/01/2017.
 */
public class Go {
    static final Logger LOG = LoggerFactory.getLogger(Run.class);
    static final int BATCH_SIZE = 1000;

    public static void main(String[] args) throws IOException, Exception {
        Connection con = null;
        try {
            con = new Driver().connect("jdbc:drill:zk=maprdemo:5181/drill/demo_mapr_com-drillbits", getDefaultProperties());
            Statement stmt = con.createStatement();
            /*
            System.out.println("Getting number of rows");
            String query = "SELECT count(*) from hive.qwi";
            ResultSet rs = stmt.executeQuery(query);

            rs.next();
            long recordCount = rs.getLong(2);

            System.out.println("Number of rows: " + recordCount);
            */

            String query = "SELECT * from `oracle.MAPR`.ACAD";
            ResultSet rs = stmt.executeQuery(query);
            rs.setFetchSize(2);
            while (rs.next()) {
                System.out.println(rs.getString(2));
            }

            
        } catch (Exception ex) {
            System.out.println(ex);
        } finally {
            if (con != null) {
                con.close();
            }
        }
        System.exit(0);
    }

    public static Properties getDefaultProperties() {
        final Properties properties = new Properties();
        properties.setProperty(ExecConstants.HTTP_ENABLE, "false");
        return properties;
    }
}
