package com.dalabs.droop;

import com.dalabs.droop.util.OptionsFileUtil;
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

    public static final String DROOP_OPTIONS_FILE_SPECIFIER = "--options-file";

    public static void main(String[] args) throws IOException, Exception {
        final String message = "USAGE:\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n";
        //Preconditions.checkArgument(args.length > 1, message);

        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception ex) {
            LOG.error("Error while expanding arguments", ex);
            System.err.println(ex.getMessage());
            System.err.println("Try 'droop help' for usage.");
            //return 1;
        }

        switch (args[0]) {
            case "producer":
                break;
            case "consumer":
                break;
            case "ListTables":
                ListTables listTables = getListTables(args);
                listTables.run();
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }

        System.exit(0);
    }

    private static ListTables getListTables(final String args[]) {

        return new ListTables(JDBC_DRIVER, DB_URL, USER, PASS);
    }
}
