package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */

import com.dalabs.droop.Droop2Options;
import com.dalabs.droop.Droop2Options.FileLayout;
import com.dalabs.droop.Droop2Options.InvalidOptionsException;
import com.dalabs.droop.cli.RelatedOptions;
import com.dalabs.droop.cli.ToolOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/*
import com.cloudera.sqoop.Droop2Options;
import com.cloudera.sqoop.Droop2Options.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
*/

import com.dalabs.droop.util.ImportException;

/**
 * Bit that lists available tables in a database.
 */
public class ImportAllTablesBit extends com.dalabs.droop.tool.ImportBit {

    public static final Log LOG = LogFactory.getLog(
        ImportAllTablesBit.class.getName());

    public ImportAllTablesBit() {
        super("import-all-tables", true);
    }

    @Override
    /** $@inheritDoc} */
    public int run(Droop2Options options) {

        if (!init(options)) {
            return 1;
        }

        try {
            String[] tables = listTables(options);
            if (null == tables) {
                System.err.println("Could not retrieve tables list from server");
                LOG.error("listTables() returned null");
                return 1;
            } else {
                for (String tableName : tables) {
                    System.out.println("Importing table: " + tableName);
                    importTable(options, tableName);
                }
            }
        } finally {
            destroy(options);
        }

        return 0;
    }

    protected String[] listTables(Droop2Options options) {
        String schemaName = options.getInputSchemaName();

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

    @Override
    @SuppressWarnings("static-access")
    /** {@inheritDoc} */
    protected RelatedOptions getImportOptions() {
        // Imports
        RelatedOptions importOpts = super.getImportOptions();

        /*
        importOpts.addOption(OptionBuilder.withArgName("tables")
                .hasArg().withDescription("Tables to exclude when importing all tables")
                .withLongOpt(ALL_TABLE_EXCLUDES_ARG)
                .create());
        */

        return importOpts;
    }

    @Override
    /** {@inheritDoc} */
    public void applyOptions(CommandLine in, Droop2Options out)
            throws InvalidOptionsException {
        super.applyOptions(in, out);

        /*
        if (in.hasOption(ALL_TABLE_EXCLUDES_ARG)) {
            out.setAllTablesExclude(in.getOptionValue(ALL_TABLE_EXCLUDES_ARG));
        }
        */
    }

}
