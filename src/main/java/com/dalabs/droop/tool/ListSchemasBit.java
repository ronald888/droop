package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */

import com.dalabs.droop.DroopOptions;
import com.dalabs.droop.DroopOptions.InvalidOptionsException;
import com.dalabs.droop.cli.ToolOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/*
import com.cloudera.sqoop.DroopOptions;
import com.cloudera.sqoop.DroopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
*/

/**
 * Bit that lists available tables in a database.
 */
public class ListSchemasBit extends BaseDroopBit {

    public static final Log LOG = LogFactory.getLog(
        ListSchemasBit.class.getName());

    /**
     * Query to list all tables visible to the current user. Note that this list
     * does not identify the table owners which is required in order to
     * ensure that the table can be operated on for import/export purposes.
     */
    public static final String QUERY_LIST_SCHEMAS =
        "SELECT SCHEMA_NAME FROM "
      + "INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME";

    public static final String SCHEMA_COLUMN_NAME = "SCHEMA_NAME";

    public ListSchemasBit() {
        super("list-schemas");
    }

    @Override
    /** $@inheritDoc} */
    public int run(DroopOptions options) {
        if (!init(options)) {
            return 1;
        }
        try {
            String [] schemas = listSchemas();
            if (null == schemas) {
                System.err.println("Could not retrieve schema list from server");
                LOG.error("listSchemas() returned null");
                return 1;
            } else {
                for (String sch : schemas) {
                    System.out.println(sch);
                }
            }
        } finally {
            destroy(options);
        }

        return 0;
    }

    protected String[] listSchemas() {
        List<String> schemas = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERY_LIST_SCHEMAS);

            while(rs.next()) {
                schemas.add(rs.getString(1));
            }
        } catch (SQLException se) {
            LOG.error("Failed to list schemas: " + StringUtils.stringifyException(se));
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

        return schemas.toArray(new String[schemas.size()]);
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(getCommonOptions());
    }

    @Override
    public void applyOptions(CommandLine in, DroopOptions out)
        throws InvalidOptionsException {
        applyCommonOptions(in, out);
    }

    @Override
    /** {@inheritDoc} */
    public void validateOptions(DroopOptions options)
            throws InvalidOptionsException {
        options.setExtraArgs(getSubcommandArgs(extraArguments));
        int dashPos = getDashPosition(extraArguments);
        if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
            throw new InvalidOptionsException(HELP_STR);
        }

        validateCommonOptions(options);
    }
}
