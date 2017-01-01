package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */
import org.apache.commons.cli.OptionBuilder;
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

import com.dalabs.droop.cli.RelatedOptions;
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
            String [] tables = listTables(options);
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

    protected String[] listTables(Droop2Options options) {
        String schemaName = options.getSchemaName();

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT TABLE_NAME ");
        sb.append("FROM INFORMATION_SCHEMA.`TABLES` ");
        sb.append("WHERE TABLE_SCHEMA = '");
        sb.append(schemaName);
        sb.append("' and TABLE_TYPE = 'TABLE' ");
        sb.append("ORDER BY TABLE_NAME ASC");

        String sqlCmd = sb.toString();
        LOG.debug("Listing tables with command: " + sqlCmd);

        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlCmd);

            while(rs.next()) {
                tables.add(rs.getString(1));
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


    /**
     * Construct the set of options that control imports, either of one
     * table or a batch of tables.
     * @return the RelatedOptions that can be used to parse the import
     * arguments.
     */
    @SuppressWarnings("static-access")
    protected RelatedOptions getListTablesOptions() {
        // Imports
        RelatedOptions listOpts = new RelatedOptions("List tables arguments");

        listOpts.addOption(OptionBuilder.withArgName("schema-name")
                .hasArg().withDescription("Schema to read")
                .withLongOpt(SCHEMA_ARG)
                .create());
        return listOpts;
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(getCommonOptions());
        toolOptions.addUniqueOptions(getListTablesOptions());
    }

    @Override
    public void printHelp(ToolOptions toolOptions) {
        super.printHelp(toolOptions);
        System.out.println("");
        System.out.println("At minimum, you must specify --connect and --schema");
    }

    @Override
    public void applyOptions(CommandLine in, Droop2Options out)
        throws InvalidOptionsException {
        try {
            applyCommonOptions(in, out);

            if (in.hasOption(SCHEMA_ARG)) {
                out.setSchemaName(in.getOptionValue(SCHEMA_ARG));
            }
        } catch (NumberFormatException nfe) {
            throw new InvalidOptionsException("Error: expected numeric argument.\n"
                    + "Try --help for usage.");
        }
    }


    /**
     * Validate list-tables-specific arguments.
     * @param options the configured Droop2Options to check
     */
    protected void validateListTablesOptions(Droop2Options options)
            throws InvalidOptionsException {
        if (options.getSchemaName() == null) {
            throw new InvalidOptionsException(
                    "--schema is required for list-tables. "
                            + HELP_STR);
        }
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

        validateListTablesOptions(options);
        validateCommonOptions(options);
    }
}
