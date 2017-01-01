package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
import com.cloudera.sqoop.Droop2Options;
import com.cloudera.sqoop.Droop2Options.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
*/

import com.dalabs.droop.cli.ToolOptions;
import com.dalabs.droop.Droop2Options;
import com.dalabs.droop.Droop2Options.InvalidOptionsException;
import org.apache.hadoop.util.StringUtils;

/**
 * Bit that lists available tables in a database.
 */
public class ListTablesBit extends BaseDroopBit {

    public static final Log LOG = LogFactory.getLog(
        ListTablesBit.class.getName());

    /**
     * Query to list all tables visible to the current user. Note that this list
     * does not identify the table owners which is required in order to
     * ensure that the table can be operated on for import/export purposes.
     */
    public static final String QUERY_LIST_TABLES =
        "SELECT TABLE_NAME "
      + "FROM INFORMATION_SCHEMA.`TABLES` "
      + "WHERE TABLE_SCHEMA = 'oracle.MAPR' and TABLE_TYPE = 'TABLE' "
      + "ORDER BY TABLE_NAME ASC";

    public static final String TABLE_COLUMN_NAME = "TABLE_NAME";

    public ListTablesBit() {
        super("list-tables");
    }

    @Override
    /** $@inheritDoc} */
    public int run(Droop2Options options) {
        if (!init(options)) {
            return 1;
        }
        try {
            String [] tables = listTables();
            if (null == tables) {
                System.err.println("Could not retrieve tables list from server");
                LOG.error("listTables() returned null");
                return 1;
            } else {
                for (String tbl : tables) {
                    System.out.println(tbl);
                }
            }
        } finally {
            destroy(options);
        }

        return 0;
    }

    protected String[] listTables() {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERY_LIST_TABLES);

            while(rs.next()) {
                tables.add(rs.getString(TABLE_COLUMN_NAME));
            }
        } catch (SQLException se) {
            LOG.error("Failed to list tables: " + StringUtils.stringifyException(se));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close resultset: " + StringUtils.stringifyException(ex));
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close statement: " + StringUtils.stringifyException(ex));
            }
        }

        return tables.toArray(new String[tables.size()]);
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(getCommonOptions());
    }

    @Override
    public void applyOptions(CommandLine in, Droop2Options out)
        throws InvalidOptionsException {
        applyCommonOptions(in, out);
    }

    @Override
    /** {@inheritDoc} */
    public void validateOptions(Droop2Options options)
            throws InvalidOptionsException {
        options.setExtraArgs(getSubcommandArgs(extraArguments));
        int dashPos = getDashPosition(extraArguments);
        if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
            throw new InvalidOptionsException(HELP_STR);
        }

        validateCommonOptions(options);
    }
}
