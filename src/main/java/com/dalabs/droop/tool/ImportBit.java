package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */

import com.dalabs.droop.Droop2Options;
import com.dalabs.droop.Droop2Options.InvalidOptionsException;
import com.dalabs.droop.cli.ToolOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;

/*
import com.cloudera.sqoop.Droop2Options;
import com.cloudera.sqoop.Droop2Options.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
*/

import com.dalabs.droop.cli.RelatedOptions;
import com.dalabs.droop.cli.ToolOptions;
import com.dalabs.droop.util.ImportException;
import com.dalabs.droop.Droop2Options.FileLayout;

/**
 * Bit that lists available tables in a database.
 */
public class ImportBit extends BaseDroopBit {

    public static final Log LOG = LogFactory.getLog(
        ImportBit.class.getName());

    public static final String SET_STORE_FORMAT =
            "ALTER SESSION SET `store.format`=";

    public static final String QUERY_LIST_TABLES =
            "SELECT TABLE_NAME "
          + "FROM INFORMATION_SCHEMA.`TABLES` "
          + "WHERE TABLE_SCHEMA = 'oracle.MAPR' and TABLE_TYPE = 'TABLE' "
          + "ORDER BY TABLE_NAME ASC";

    // true if this is an all-tables import. Set by a subclass which
    // overrides the run() method of this tool (which can only do
    // a single table).
    private boolean allTables;

    // store check column type for incremental option
    private int checkColumnType;

    public ImportBit() {
        this("import", false);
    }

    public ImportBit(String bitName, boolean allTables) {
        super(bitName);
        this.allTables = allTables;
    }

    @Override
    protected boolean init(Droop2Options droopOpts) {
        boolean ret = super.init(droopOpts);
        return ret;
    }

    /**
     * TODO: Handle incrementals
     * @return true if the supplied options specify an incremental import.
     */
    /*
    private boolean isIncremental(Droop2Options options) {
        return !options.getIncre
    }
    */

    @Override
    /** $@inheritDoc} */
    public int run(Droop2Options options) {
        if (allTables) {
            // We got into this method, but we should be in a subclass.
            // (This method only handles a single table)
            // This should not be reached, but for sanity's sake, test here.
            LOG.error("ImportBit.run() can only handle a single table.");
            return 1;
        }

        if (!init(options)) {
            return 1;
        }

        try {
            if (!setStoreFormat(options)) {
                System.err.println("Can not set store format");
                LOG.error("setStoreFormat() returned false");
                return 1;
            }

            if (!importTable(options)) {
                System.err.println("Can not import table");
                LOG.error("importTable() returned false");
                return 1;
            }
        } finally {
            destroy(options);
        }

        return 0;
    }

    protected boolean importTable(Droop2Options options) {
        // TODO: apply getOutputPath
        String hdfsTargetDir = options.getTargetDir();
        String schemaName = options.getSchemaName();
        String tableName = options.getTableName();

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(hdfsTargetDir);
        sb.append(" AS ");
        sb.append("SELECT * FROM `");
        sb.append(schemaName);
        sb.append("`.`");
        sb.append(tableName);
        sb.append("`");

        String sqlCmd = sb.toString();
        LOG.debug("Importing data with command: " + sqlCmd);

        Statement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlCmd);
            rs.next();
            LOG.info("Total number of records imported: " + rs.getString(2));
            System.out.println("Total number of records imported: " + rs.getString(2));
        } catch (SQLException se) {
            LOG.error("Failed to import table: " + StringUtils.stringifyException(se));
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close resultset: " + StringUtils.stringifyException(ex));
                return false;
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close statement: " + StringUtils.stringifyException(ex));
                return false;
            }
        }

        return true;
    }

    protected boolean setStoreFormat(Droop2Options options) {
        String storeFormat = getStoreFormat(options.getFileLayout());

        StringBuilder sb = new StringBuilder();
        sb.append(SET_STORE_FORMAT);
        sb.append(storeFormat);

        Statement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        String sqlCmd = sb.toString();
        LOG.debug("Setting Store Format: " + sqlCmd);

        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sqlCmd);
            rs.next();
            LOG.info("Store Format Updated: " + storeFormat);
            System.out.println("Store Format Updated: " + storeFormat);
        } catch (SQLException se) {
            LOG.error("Failed to set store format: " + StringUtils.stringifyException(se));
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close resultset: " + StringUtils.stringifyException(ex));
                return false;
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                LOG.error("Failed to close statement: " + StringUtils.stringifyException(ex));
                return false;
            }
        }

        return true;
    }

    protected String getStoreFormat(FileLayout layout) {
        switch (layout) {
            case TextFile:
                return "'csv'";
            case ParquetFile:
                return "'parquet'";
            default:
                throw new IllegalArgumentException("Don't know how to do " + layout.toString());
        }
    }

    /**
     * Construct the set of options that control imports, either of one
     * table or a batch of tables.
     * @return the RelatedOptions that can be used to parse the import
     * arguments.
     */
    @SuppressWarnings("static-access")
    protected RelatedOptions getImportOptions() {
        // Imports
        RelatedOptions importOpts = new RelatedOptions("Import control arguments");

        /*
        importOpts.addOption(OptionBuilder
                .withDescription("Use direct import fast path")
                .withLongOpt(DIRECT_ARG)
                .create());
        */

        if (!allTables) {
            importOpts.addOption(OptionBuilder.withArgName("schema-name")
                    .hasArg().withDescription("Schema to read")
                    .withLongOpt(SCHEMA_ARG)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("table-name")
                    .hasArg().withDescription("Table to read")
                    .withLongOpt(TABLE_ARG)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("col,col,col...")
                    .hasArg().withDescription("Columns to import from table")
                    .withLongOpt(COLUMNS_ARG)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("where clause")
                    .hasArg().withDescription("WHERE clause to use during import")
                    .withLongOpt(WHERE_ARG)
                    .create());
            importOpts.addOption(OptionBuilder
                    .withDescription("Imports data in append mode")
                    .withLongOpt(APPEND_ARG)
                    .create());
            importOpts.addOption(OptionBuilder
                    .withDescription("Imports data in delete mode")
                    .withLongOpt(DELETE_ARG)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("dir")
                    .hasArg().withDescription("HDFS plain table destination")
                    .withLongOpt(TARGET_DIR_ARG)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("statement")
                    .hasArg()
                    .withDescription("Import results of SQL 'statement'")
                    .withLongOpt(SQL_QUERY_ARG)
                    .create(SQL_QUERY_SHORT_ARG));
            importOpts.addOption(OptionBuilder.withArgName("statement")
                    .hasArg()
                    .withDescription("Set boundary query for retrieving max and min"
                            + " value of the primary key")
                    .withLongOpt(SQL_QUERY_BOUNDARY)
                    .create());
            importOpts.addOption(OptionBuilder.withArgName("column")
                    .hasArg().withDescription("Key column to use to join results")
                    .withLongOpt(MERGE_KEY_ARG)
                    .create());

            addValidationOpts(importOpts);
        }

        importOpts.addOption(OptionBuilder.withArgName("dir")
                .hasArg().withDescription("HDFS parent for table destination")
                .withLongOpt(WAREHOUSE_DIR_ARG)
                .create());
        importOpts.addOption(OptionBuilder
                .withDescription("Imports data to SequenceFiles")
                .withLongOpt(FMT_SEQUENCEFILE_ARG)
                .create());
        importOpts.addOption(OptionBuilder
                .withDescription("Imports data as plain text (default)")
                .withLongOpt(FMT_TEXTFILE_ARG)
                .create());
        importOpts.addOption(OptionBuilder
                .withDescription("Imports data to Avro data files")
                .withLongOpt(FMT_AVRODATAFILE_ARG)
                .create());
        importOpts.addOption(OptionBuilder
                .withDescription("Imports data to Parquet files")
                .withLongOpt(BaseDroopBit.FMT_PARQUETFILE_ARG)
                .create());
        importOpts.addOption(OptionBuilder.withArgName("n")
                .hasArg().withDescription("Use 'n' map tasks to import in parallel")
                .withLongOpt(NUM_MAPPERS_ARG)
                .create(NUM_MAPPERS_SHORT_ARG));
        importOpts.addOption(OptionBuilder.withArgName("name")
                .hasArg().withDescription("Set name for generated mapreduce job")
                .withLongOpt(MAPREDUCE_JOB_NAME)
                .create());
        importOpts.addOption(OptionBuilder
                .withDescription("Enable compression")
                .withLongOpt(COMPRESS_ARG)
                .create(COMPRESS_SHORT_ARG));
        importOpts.addOption(OptionBuilder.withArgName("codec")
                .hasArg()
                .withDescription("Compression codec to use for import")
                .withLongOpt(COMPRESSION_CODEC_ARG)
                .create());
        importOpts.addOption(OptionBuilder.withArgName("n")
                .hasArg()
                .withDescription("Split the input stream every 'n' bytes "
                        + "when importing in direct mode")
                .withLongOpt(DIRECT_SPLIT_SIZE_ARG)
                .create());
        importOpts.addOption(OptionBuilder.withArgName("n")
                .hasArg()
                .withDescription("Set the maximum size for an inline LOB")
                .withLongOpt(INLINE_LOB_LIMIT_ARG)
                .create());
        importOpts.addOption(OptionBuilder.withArgName("n")
                .hasArg()
                .withDescription("Set number 'n' of rows to fetch from the "
                        + "database when more rows are needed")
                .withLongOpt(FETCH_SIZE_ARG)
                .create());
        importOpts.addOption(OptionBuilder.withArgName("reset-mappers")
                .withDescription("Reset the number of mappers to one mapper if no split key available")
                .withLongOpt(AUTORESET_TO_ONE_MAPPER)
                .create());
        return importOpts;
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(getCommonOptions());
        toolOptions.addUniqueOptions(getImportOptions());
    }

    @Override
    public void printHelp(ToolOptions toolOptions) {
        super.printHelp(toolOptions);
        System.out.println("");
        if (allTables) {
            System.out.println("At minimum, you must specify --connect and --schema");
        } else {
            System.out.println("At minimum, you must specify --connect, --schema and --table");
        }
    }

    @Override
    public void applyOptions(CommandLine in, Droop2Options out)
        throws InvalidOptionsException {
        try {
            applyCommonOptions(in, out);

            if (!allTables) {
                if (in.hasOption(TABLE_ARG)) {
                    out.setTableName(in.getOptionValue(TABLE_ARG));
                }

                if (in.hasOption(COLUMNS_ARG)) {
                    String[] cols= in.getOptionValue(COLUMNS_ARG).split(",");
                    for (int i=0; i<cols.length; i++) {
                        cols[i] = cols[i].trim();
                    }
                    out.setColumns(cols);
                }

                if (in.hasOption(WHERE_ARG)) {
                    out.setWhereClause(in.getOptionValue(WHERE_ARG));
                }

                if (in.hasOption(TARGET_DIR_ARG)) {
                    out.setTargetDir(in.getOptionValue(TARGET_DIR_ARG));
                }

                if (in.hasOption(APPEND_ARG)) {
                    out.setAppendMode(true);
                }

                if (in.hasOption(DELETE_ARG)) {
                    out.setDeleteMode(true);
                }

                /*
                if (in.hasOption(SQL_QUERY_ARG)) {
                    out.setSqlQuery(in.getOptionValue(SQL_QUERY_ARG));
                }

                if (in.hasOption(SQL_QUERY_BOUNDARY)) {
                    out.setBoundaryQuery(in.getOptionValue(SQL_QUERY_BOUNDARY));
                }

                if (in.hasOption(MERGE_KEY_ARG)) {
                    out.setMergeKeyCol(in.getOptionValue(MERGE_KEY_ARG));
                }
                */

                // applyValidationOptions(in, out);
            }

            if (in.hasOption(SCHEMA_ARG)) {
                out.setSchemaName(in.getOptionValue(SCHEMA_ARG));
            }

            if (in.hasOption(WAREHOUSE_DIR_ARG)) {
                out.setWarehouseDir(in.getOptionValue(WAREHOUSE_DIR_ARG));
            }

            if (in.hasOption(FMT_SEQUENCEFILE_ARG)) {
                out.setFileLayout(Droop2Options.FileLayout.SequenceFile);
            }

            if (in.hasOption(FMT_TEXTFILE_ARG)) {
                out.setFileLayout(Droop2Options.FileLayout.TextFile);
            }

            if (in.hasOption(FMT_AVRODATAFILE_ARG)) {
                out.setFileLayout(Droop2Options.FileLayout.AvroDataFile);
            }

            if (in.hasOption(FMT_PARQUETFILE_ARG)) {
                out.setFileLayout(Droop2Options.FileLayout.ParquetFile);
            }
        } catch (NumberFormatException nfe) {
            throw new InvalidOptionsException("Error: expected numeric argument.\n"
                + "Try --help for usage.");
        }
    }

    /**
     * Validate import-specific arguments.
     * @param options the configured Droop2Options to check
     */
    protected void validateImportOptions(Droop2Options options)
            throws InvalidOptionsException {
         if (!allTables  &&
                 (options.getSchemaName() == null
                 || options.getTableName() == null)) {
             throw new InvalidOptionsException(
                     "--schema and --table are required for import. "
                             + "(Or use droop import-all-tables.)"
                             + HELP_STR);
         } else if (allTables && options.getSchemaName() == null) {
             throw new InvalidOptionsException(
                     "--schema is required for import-all-tables."
                             + HELP_STR);
        // if (!allTables && options.getTableName() == null
        //         && options.getSqlQuery() == null) {
        //    throw new InvalidOptionsException(
        //            "--table or --" + SQL_QUERY_ARG + " is required for import. "
        //                    + "(Or use droop import-all-tables.)"
        //                    + HELP_STR);
        } else if (options.getTargetDir() != null
                && options.getWarehouseDir() != null) {
            throw new InvalidOptionsException(
                    "--target-dir with --warehouse-dir are incompatible options."
                            + HELP_STR);
        /*
        } else if (options.getTableName() != null
                && options.getSqlQuery() != null) {
            throw new InvalidOptionsException(
                    "Cannot specify --" + SQL_QUERY_ARG + " and --table together."
                            + HELP_STR);
        */
        } else if (options.getTargetDir() == null) {
        /*
        } else if (options.getSqlQuery() != null
                && options.getTargetDir() == null
                && options.getHBaseTable() == null
                && options.getHCatTableName() == null
                && options.getAccumuloTable() == null) {
        */
            throw new InvalidOptionsException(
                    "Must specify destination with --target-dir. "
                            + HELP_STR);
        /*
        } else if (options.getSqlQuery() != null && options.doHiveImport()
                && options.getHiveTableName() == null) {
            throw new InvalidOptionsException(
                    "When importing a query to Hive, you must specify --"
                            + HIVE_TABLE_ARG + "." + HELP_STR);
        } else if (options.getSqlQuery() != null && options.getNumMappers() > 1
                && options.getSplitByCol() == null) {
            throw new InvalidOptionsException(
                    "When importing query results in parallel, you must specify --"
                            + SPLIT_BY_ARG + "." + HELP_STR);
        } else if (options.isDirect()) {
            validateDirectImportOptions(options);
        */
        /*
        } else if (allTables && options.isValidationEnabled()) {
            throw new InvalidOptionsException("Validation is not supported for "
                    + "all tables but single table only.");
        } else if (options.getSqlQuery() != null && options.isValidationEnabled()) {
            throw new InvalidOptionsException("Validation is not supported for "
                    + "free from query but single table only.");
        } else if (options.getWhereClause() != null
                && options.isValidationEnabled()) {
            throw new InvalidOptionsException("Validation is not supported for "
                    + "where clause but single table only.");
        } else if (options.getIncrementalMode()
                != Droop2Options.IncrementalMode.None && options.isValidationEnabled()) {
            throw new InvalidOptionsException("Validation is not supported for "
                    + "incremental imports but single table only.");
        } else if ((options.getTargetDir() != null
                || options.getWarehouseDir() != null)
                && options.getHCatTableName() != null) {
            throw new InvalidOptionsException("--hcatalog-table cannot be used "
                    + " --warehouse-dir or --target-dir options");
        */
        } else if (options.isDeleteMode() && options.isAppendMode()) {
            throw new InvalidOptionsException("--append and --delete-target-dir can"
                    + " not be used together.");
        /*
        } else if (options.isDeleteMode() && options.getIncrementalMode()
                != Droop2Options.IncrementalMode.None) {
            throw new InvalidOptionsException("--delete-target-dir can not be used"
                    + " with incremental imports.");
        } else if (options.getAutoResetToOneMapper()
                && (options.getSplitByCol() != null)) {
            throw new InvalidOptionsException("--autoreset-to-one-mapper and"
                    + " --split-by cannot be used together.");
        */
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

        validateImportOptions(options);
        validateCommonOptions(options);
    }
}
