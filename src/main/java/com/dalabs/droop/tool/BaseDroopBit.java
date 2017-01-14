package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.sql.*;

import com.dalabs.droop.Droop2Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import com.dalabs.droop.util.CredentialsUtil2;
import com.dalabs.droop.util.LoggingUtils;
import com.dalabs.droop.util.password.CredentialProviderHelper;

import com.dalabs.droop.Droop2Options;
import com.dalabs.droop.Droop2Options.InvalidOptionsException;
import com.dalabs.droop.cli.RelatedOptions;
import com.dalabs.droop.cli.ToolOptions;

public abstract class BaseDroopBit extends com.dalabs.droop.tool.DroopBit {

    public static final String METADATA_TRANSACTION_ISOLATION_LEVEL = "metadata-transaction-isolation-level";

    public static final Log LOG = LogFactory.getLog(
            BaseDroopBit.class.getName());

    public static final String HELP_STR = "\nTry --help for usage instructions.";
    public static final String DEFAULT_JDBC_DRIVER = "org.apache.drill.jdbc.Driver";

    protected Connection conn = null;

    public Connection getConnection() {
        return conn;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    // TODO: Clean up needed
    // Here are all the arguments that are used by the standard droop tools.
    // Their names are recorded here so that tools can share them and their
    // use consistently. The argument parser applies the leading '--' to each
    // string.
    public static final String CONNECT_STRING_ARG = "connect";
    public static final String CONN_MANAGER_CLASS_NAME =
            "connection-manager";
    public static final String CONNECT_PARAM_FILE = "connection-param-file";
    public static final String DRIVER_ARG = "driver";
    public static final String USERNAME_ARG = "username";
    public static final String PASSWORD_ARG = "password";
    public static final String PASSWORD_PROMPT_ARG = "P";
    public static final String PASSWORD_PATH_ARG = "password-file";
    public static final String PASSWORD_ALIAS_ARG = "password-alias";
    public static final String DIRECT_ARG = "direct";
    public static final String BATCH_ARG = "batch";
    public static final String SCHEMA_ARG = "schema";
    public static final String INPUT_SCHEMA_ARG = "input-schema";
    public static final String OUTPUT_SCHEMA_ARG = "output-schema";
    public static final String TABLE_ARG = "table";
    public static final String STAGING_TABLE_ARG = "staging-table";
    public static final String CLEAR_STAGING_TABLE_ARG = "clear-staging-table";
    public static final String COLUMNS_ARG = "columns";
    public static final String SPLIT_BY_ARG = "split-by";
    public static final String SPLIT_LIMIT_ARG = "split-limit";
    public static final String WHERE_ARG = "where";
    public static final String HADOOP_HOME_ARG = "hadoop-home";
    public static final String HADOOP_MAPRED_HOME_ARG = "hadoop-mapred-home";
    public static final String HIVE_HOME_ARG = "hive-home";
    public static final String WAREHOUSE_DIR_ARG = "warehouse-dir";
    public static final String TARGET_DIR_ARG = "target-dir";
    public static final String APPEND_ARG = "append";
    public static final String DELETE_ARG = "delete-target-dir";
    public static final String NULL_STRING = "null-string";
    public static final String INPUT_NULL_STRING = "input-null-string";
    public static final String NULL_NON_STRING = "null-non-string";
    public static final String INPUT_NULL_NON_STRING = "input-null-non-string";
    public static final String MAP_COLUMN_JAVA = "map-column-java";
    public static final String MAP_COLUMN_HIVE = "map-column-hive";

    public static final String FMT_SEQUENCEFILE_ARG = "as-sequencefile";
    public static final String FMT_TEXTFILE_ARG = "as-textfile";
    public static final String FMT_AVRODATAFILE_ARG = "as-avrodatafile";
    public static final String FMT_PARQUETFILE_ARG = "as-parquetfile";
    public static final String HIVE_IMPORT_ARG = "hive-import";
    public static final String HIVE_TABLE_ARG = "hive-table";
    public static final String HIVE_DATABASE_ARG = "hive-database";
    public static final String HIVE_OVERWRITE_ARG = "hive-overwrite";
    public static final String HIVE_DROP_DELIMS_ARG = "hive-drop-import-delims";
    public static final String HIVE_DELIMS_REPLACEMENT_ARG =
            "hive-delims-replacement";
    public static final String HIVE_PARTITION_KEY_ARG = "hive-partition-key";
    public static final String HIVE_PARTITION_VALUE_ARG = "hive-partition-value";
    public static final String HCATCALOG_PARTITION_KEYS_ARG =
            "hcatalog-partition-keys";
    public static final String HCATALOG_PARTITION_VALUES_ARG =
            "hcatalog-partition-values";
    public static final String CREATE_HIVE_TABLE_ARG =
            "create-hive-table";
    public static final String HCATALOG_TABLE_ARG = "hcatalog-table";
    public static final String HCATALOG_DATABASE_ARG = "hcatalog-database";
    public static final String CREATE_HCATALOG_TABLE_ARG =
            "create-hcatalog-table";
    public static final String DROP_AND_CREATE_HCATALOG_TABLE =
            "drop-and-create-hcatalog-table";
    public static final String HCATALOG_STORAGE_STANZA_ARG =
            "hcatalog-storage-stanza";
    public static final String HCATALOG_HOME_ARG = "hcatalog-home";
    public static final String MAPREDUCE_JOB_NAME = "mapreduce-job-name";
    public static final String NUM_MAPPERS_ARG = "num-mappers";
    public static final String NUM_MAPPERS_SHORT_ARG = "m";
    public static final String COMPRESS_ARG = "compress";
    public static final String COMPRESSION_CODEC_ARG = "compression-codec";
    public static final String COMPRESS_SHORT_ARG = "z";
    public static final String DIRECT_SPLIT_SIZE_ARG = "direct-split-size";
    public static final String INLINE_LOB_LIMIT_ARG = "inline-lob-limit";
    public static final String FETCH_SIZE_ARG = "fetch-size";
    public static final String EXPORT_PATH_ARG = "export-dir";
    public static final String FIELDS_TERMINATED_BY_ARG = "fields-terminated-by";
    public static final String LINES_TERMINATED_BY_ARG = "lines-terminated-by";
    public static final String OPTIONALLY_ENCLOSED_BY_ARG =
            "optionally-enclosed-by";
    public static final String ENCLOSED_BY_ARG = "enclosed-by";
    public static final String ESCAPED_BY_ARG = "escaped-by";
    public static final String MYSQL_DELIMITERS_ARG = "mysql-delimiters";
    public static final String INPUT_FIELDS_TERMINATED_BY_ARG =
            "input-fields-terminated-by";
    public static final String INPUT_LINES_TERMINATED_BY_ARG =
            "input-lines-terminated-by";
    public static final String INPUT_OPTIONALLY_ENCLOSED_BY_ARG =
            "input-optionally-enclosed-by";
    public static final String INPUT_ENCLOSED_BY_ARG = "input-enclosed-by";
    public static final String INPUT_ESCAPED_BY_ARG = "input-escaped-by";
    public static final String CODE_OUT_DIR_ARG = "outdir";
    public static final String BIN_OUT_DIR_ARG = "bindir";
    public static final String PACKAGE_NAME_ARG = "package-name";
    public static final String CLASS_NAME_ARG = "class-name";
    public static final String JAR_FILE_NAME_ARG = "jar-file";
    public static final String SQL_QUERY_ARG = "query";
    public static final String SQL_QUERY_BOUNDARY = "boundary-query";
    public static final String SQL_QUERY_SHORT_ARG = "e";
    public static final String VERBOSE_ARG = "verbose";
    public static final String HELP_ARG = "help";
    public static final String TEMP_ROOTDIR_ARG = "temporary-rootdir";
    public static final String UPDATE_KEY_ARG = "update-key";
    public static final String UPDATE_MODE_ARG = "update-mode";
    public static final String CALL_ARG = "call";
    public static final String SKIP_DISTCACHE_ARG = "skip-dist-cache";
    public static final String RELAXED_ISOLATION = "relaxed-isolation";
    public static final String THROW_ON_ERROR_ARG = "throw-on-error";
    public static final String ORACLE_ESCAPING_DISABLED = "oracle-escaping-disabled";

    // Arguments for validation.
    public static final String VALIDATE_ARG = "validate";
    public static final String VALIDATOR_CLASS_ARG = "validator";
    public static final String VALIDATION_THRESHOLD_CLASS_ARG =
            "validation-threshold";
    public static final String VALIDATION_FAILURE_HANDLER_CLASS_ARG =
            "validation-failurehandler";

    // Arguments for incremental imports.
    public static final String INCREMENT_TYPE_ARG = "incremental";
    public static final String INCREMENT_COL_ARG = "check-column";
    public static final String INCREMENT_LAST_VAL_ARG = "last-value";

    // Arguments for all table imports.
    public static final String ALL_TABLE_EXCLUDES_ARG = "exclude-tables";

    // HBase arguments.
    public static final String HBASE_TABLE_ARG = "hbase-table";
    public static final String HBASE_COL_FAM_ARG = "column-family";
    public static final String HBASE_ROW_KEY_ARG = "hbase-row-key";
    public static final String HBASE_BULK_LOAD_ENABLED_ARG =
            "hbase-bulkload";
    public static final String HBASE_CREATE_TABLE_ARG = "hbase-create-table";

    //Accumulo arguments.
    public static final String ACCUMULO_TABLE_ARG = "accumulo-table";
    public static final String ACCUMULO_COL_FAM_ARG = "accumulo-column-family";
    public static final String ACCUMULO_ROW_KEY_ARG = "accumulo-row-key";
    public static final String ACCUMULO_VISIBILITY_ARG = "accumulo-visibility";
    public static final String ACCUMULO_CREATE_TABLE_ARG
            = "accumulo-create-table";
    public static final String ACCUMULO_BATCH_SIZE_ARG = "accumulo-batch-size";
    public static final String ACCUMULO_MAX_LATENCY_ARG = "accumulo-max-latency";
    public static final String ACCUMULO_ZOOKEEPERS_ARG = "accumulo-zookeepers";
    public static final String ACCUMULO_INSTANCE_ARG = "accumulo-instance";
    public static final String ACCUMULO_USER_ARG = "accumulo-user";
    public static final String ACCUMULO_PASSWORD_ARG = "accumulo-password";


    // Arguments for the saved job management system.
    public static final String STORAGE_METASTORE_ARG = "meta-connect";
    public static final String JOB_CMD_CREATE_ARG = "create";
    public static final String JOB_CMD_DELETE_ARG = "delete";
    public static final String JOB_CMD_EXEC_ARG = "exec";
    public static final String JOB_CMD_LIST_ARG = "list";
    public static final String JOB_CMD_SHOW_ARG = "show";

    // Arguments for the metastore.
    public static final String METASTORE_SHUTDOWN_ARG = "shutdown";


    // Arguments for merging datasets.
    public static final String NEW_DATASET_ARG = "new-data";
    public static final String OLD_DATASET_ARG = "onto";
    public static final String MERGE_KEY_ARG = "merge-key";

    // Reset number of mappers to one if there is no primary key avaliable and
    // split by column is explicitly not provided

    public static final String AUTORESET_TO_ONE_MAPPER = "autoreset-to-one-mapper";

    static final String HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED = "--incremental lastmodified option for hive imports is not "
            + "supported. Please remove the parameter --incremental lastmodified.";

    public BaseDroopBit() {
    }

    public BaseDroopBit(String bitName) {
        super(bitName);
    }

    protected boolean init(Droop2Options droopOpts) {
        droopOpts.setBitName(getBitName());

        /** Check if the JDBC Driver Class has not been set **/
        if (droopOpts.getDriverClassName() == null) {
            droopOpts.setDriverClassName(DEFAULT_JDBC_DRIVER);
        }
        /**
         * TODO: set connection factory???
         * for now, set it within this class
         */
        try {
            Class.forName(droopOpts.getDriverClassName());
            conn = DriverManager.getConnection(
                    droopOpts.getConnectString(),
                    droopOpts.getUsername(),
                    droopOpts.getPassword());
        } catch(SQLException se) {
            //Handle errors for JDBC
            LOG.error("Error with JDBC "
                    + droopOpts.getDriverClassName() + ":" + StringUtils.stringifyException(se));
            return false;
        } catch(Exception e) {
            //Handle errors for Class.forName
            LOG.error("Error with Class "
                    + droopOpts.getDriverClassName() + ":" + StringUtils.stringifyException(e));
            return false;
        }

        return true;
    }

    protected void rethrowIfRequired(Droop2Options options, Exception ex) {
        if (!options.isThrowOnError()) {
            return;
        }

        final RuntimeException exceptionToThrow;
        if (ex instanceof RuntimeException) {
            exceptionToThrow = (RuntimeException) ex;
        } else {
            exceptionToThrow = new RuntimeException(ex);
        }

        throw exceptionToThrow;
    }


    /**
     * Should be called in a 'finally' block at the end of the run() method.
     */
    protected void destroy(Droop2Options droopOpts) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException se) {
                LOG.error("Error with closing connection :" + StringUtils.stringifyException((se)));
            }
        }
    }

    /**
     * Examines a subset of the arrray presented, and determines if it
     * contains any non-empty arguments. If so, logs the arguments
     * and returns true.
     *
     * @param argv an array of strings to check.
     * @param offset the first element of the array to check
     * @param len the number of elements to check
     * @return true if there are any non-null, non-empty argument strings
     * present.
     */
    protected boolean hasUnrecognizedArgs(String [] argv, int offset, int len) {
        if (argv == null) {
            return false;
        }

        boolean unrecognized = false;
        boolean printedBanner = false;
        for (int i = offset; i < Math.min(argv.length, offset + len); i++) {
            if (argv[i] != null && argv[i].length() > 0) {
                if (!printedBanner) {
                    LOG.error("Error parsing arguments for " + getBitName() + ":");
                    printedBanner = true;
                }
                LOG.error("Unrecognized argument: " + argv[i]);
                unrecognized = true;
            }
        }

        return unrecognized;
    }

    protected boolean hasUnrecognizedArgs(String [] argv) {
        if (null == argv) {
            return false;
        }
        return hasUnrecognizedArgs(argv, 0, argv.length);
    }


    /**
     * If argv contains an entry "--", return an array containing all elements
     * after the "--" separator. Otherwise, return null.
     * @param argv a set of arguments to scan for the subcommand arguments.
     */
    protected String [] getSubcommandArgs(String [] argv) {
        if (null == argv) {
            return null;
        }

        for (int i = 0; i < argv.length; i++) {
            if (argv[i].equals("--")) {
                return Arrays.copyOfRange(argv, i + 1, argv.length);
            }
        }

        return null;
    }

    /**
     * @return RelatedOptions used by job management tools.
     */
    protected RelatedOptions getJobOptions() {
        RelatedOptions relatedOpts = new RelatedOptions(
                "Job management arguments");
        relatedOpts.addOption(OptionBuilder.withArgName("jdbc-uri")
                .hasArg()
                .withDescription("Specify JDBC connect string for the metastore")
                .withLongOpt(STORAGE_METASTORE_ARG)
                .create());

        // Create an option-group surrounding the operations a user
        // can perform on jobs.
        OptionGroup group = new OptionGroup();
        group.addOption(OptionBuilder.withArgName("job-id")
                .hasArg()
                .withDescription("Create a new saved job")
                .withLongOpt(JOB_CMD_CREATE_ARG)
                .create());
        group.addOption(OptionBuilder.withArgName("job-id")
                .hasArg()
                .withDescription("Delete a saved job")
                .withLongOpt(JOB_CMD_DELETE_ARG)
                .create());
        group.addOption(OptionBuilder.withArgName("job-id")
                .hasArg()
                .withDescription("Show the parameters for a saved job")
                .withLongOpt(JOB_CMD_SHOW_ARG)
                .create());

        Option execOption = OptionBuilder.withArgName("job-id")
                .hasArg()
                .withDescription("Run a saved job")
                .withLongOpt(JOB_CMD_EXEC_ARG)
                .create();
        group.addOption(execOption);

        group.addOption(OptionBuilder
                .withDescription("List saved jobs")
                .withLongOpt(JOB_CMD_LIST_ARG)
                .create());

        relatedOpts.addOptionGroup(group);

        // Since the "common" options aren't used in the job tool,
        // add these settings here.
        relatedOpts.addOption(OptionBuilder
                .withDescription("Print more information while working")
                .withLongOpt(VERBOSE_ARG)
                .create());
        relatedOpts.addOption(OptionBuilder
                .withDescription("Print usage instructions")
                .withLongOpt(HELP_ARG)
                .create());

        return relatedOpts;
    }

    /**
     * @return RelatedOptions used by most/all Droop bits.
     */
    protected RelatedOptions getCommonOptions() {
        // Connection args (common)
        RelatedOptions commonOpts = new RelatedOptions("Common arguments");
        commonOpts.addOption(OptionBuilder.withArgName("jdbc-uri")
                .hasArg().withDescription("Specify JDBC connect string")
                .withLongOpt(CONNECT_STRING_ARG)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("class-name")
                .hasArg().withDescription("Specify connection manager class name")
                .withLongOpt(CONN_MANAGER_CLASS_NAME)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("properties-file")
                .hasArg().withDescription("Specify connection parameters file")
                .withLongOpt(CONNECT_PARAM_FILE)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("class-name")
                .hasArg().withDescription("Manually specify JDBC driver class to use")
                .withLongOpt(DRIVER_ARG)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("username")
                .hasArg().withDescription("Set authentication username")
                .withLongOpt(USERNAME_ARG)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("password")
                .hasArg().withDescription("Set authentication password")
                .withLongOpt(PASSWORD_ARG)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName(PASSWORD_PATH_ARG)
                .hasArg().withDescription("Set authentication password file path")
                .withLongOpt(PASSWORD_PATH_ARG)
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Read password from console")
                .create(PASSWORD_PROMPT_ARG));
        commonOpts.addOption(OptionBuilder.withArgName(PASSWORD_ALIAS_ARG)
                .hasArg().withDescription("Credential provider password alias")
                .withLongOpt(PASSWORD_ALIAS_ARG)
                .create());
        commonOpts.addOption(OptionBuilder.withArgName("dir")
                .hasArg().withDescription("Override $HADOOP_MAPRED_HOME_ARG")
                .withLongOpt(HADOOP_MAPRED_HOME_ARG)
                .create());

        commonOpts.addOption(OptionBuilder.withArgName("hdir")
                .hasArg().withDescription("Override $HADOOP_MAPRED_HOME_ARG")
                .withLongOpt(HADOOP_HOME_ARG)
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Skip copying jars to distributed cache")
                .withLongOpt(SKIP_DISTCACHE_ARG)
                .create());

        // misc (common)
        commonOpts.addOption(OptionBuilder
                .withDescription("Print more information while working")
                .withLongOpt(VERBOSE_ARG)
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Print usage instructions")
                .withLongOpt(HELP_ARG)
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Defines the temporary root directory for the import")
                .withLongOpt(TEMP_ROOTDIR_ARG)
                .hasArg()
                .withArgName("rootdir")
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Defines the transaction isolation level for metadata queries. "
                        + "For more details check java.sql.Connection javadoc or the JDBC specificaiton")
                .withLongOpt(METADATA_TRANSACTION_ISOLATION_LEVEL)
                .hasArg()
                .withArgName("isolationlevel")
                .create());
        commonOpts.addOption(OptionBuilder
                .withDescription("Rethrow a RuntimeException on error occurred during the job")
                .withLongOpt(THROW_ON_ERROR_ARG)
                .create());
        // relax isolation requirements
        commonOpts.addOption(OptionBuilder
                .withDescription("Use read-uncommitted isolation for imports")
                .withLongOpt(RELAXED_ISOLATION)
                .create());

        commonOpts.addOption(OptionBuilder
                .withDescription("Disable the escaping mechanism of the Oracle/OraOop connection managers")
                .withLongOpt(ORACLE_ESCAPING_DISABLED)
                .hasArg()
                .withArgName("boolean")
                .create());

        return commonOpts;
    }

    /**
     * @param explicitHiveImport true if the user has an explicit --hive-import
     * available, or false if this is implied by the tool.
     * @return options governing interaction with Hive
     */
    protected RelatedOptions getHiveOptions(boolean explicitHiveImport) {
        RelatedOptions hiveOpts = new RelatedOptions("Hive arguments");
        if (explicitHiveImport) {
            hiveOpts.addOption(OptionBuilder
                    .withDescription("Import tables into Hive "
                            + "(Uses Hive's default delimiters if none are set.)")
                    .withLongOpt(HIVE_IMPORT_ARG)
                    .create());
        }

        hiveOpts.addOption(OptionBuilder.withArgName("dir")
                .hasArg().withDescription("Override $HIVE_HOME")
                .withLongOpt(HIVE_HOME_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder
                .withDescription("Overwrite existing data in the Hive table")
                .withLongOpt(HIVE_OVERWRITE_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder
                .withDescription("Fail if the target hive table exists")
                .withLongOpt(CREATE_HIVE_TABLE_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder.withArgName("table-name")
                .hasArg()
                .withDescription("Sets the table name to use when importing to hive")
                .withLongOpt(HIVE_TABLE_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder.withArgName("database-name")
                .hasArg()
                .withDescription("Sets the database name to use when importing to hive")
                .withLongOpt(HIVE_DATABASE_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder
                .withDescription("Drop Hive record \\0x01 and row delimiters "
                        + "(\\n\\r) from imported string fields")
                .withLongOpt(HIVE_DROP_DELIMS_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder
                .hasArg()
                .withDescription("Replace Hive record \\0x01 and row delimiters "
                        + "(\\n\\r) from imported string fields with user-defined string")
                .withLongOpt(HIVE_DELIMS_REPLACEMENT_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder.withArgName("partition-key")
                .hasArg()
                .withDescription("Sets the partition key to use when importing to hive")
                .withLongOpt(HIVE_PARTITION_KEY_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder.withArgName("partition-value")
                .hasArg()
                .withDescription("Sets the partition value to use when importing "
                        + "to hive")
                .withLongOpt(HIVE_PARTITION_VALUE_ARG)
                .create());
        hiveOpts.addOption(OptionBuilder
                .hasArg()
                .withDescription("Override mapping for specific column to hive"
                        + " types.")
                .withLongOpt(MAP_COLUMN_HIVE)
                .create());

        return hiveOpts;
    }

    /**
     * @return options governing interaction with HCatalog.
     */
    protected RelatedOptions getHCatalogOptions() {
        RelatedOptions hCatOptions = new RelatedOptions("HCatalog arguments");
        hCatOptions.addOption(OptionBuilder
                .hasArg()
                .withDescription("HCatalog table name")
                .withLongOpt(HCATALOG_TABLE_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder
                .hasArg()
                .withDescription("HCatalog database name")
                .withLongOpt(HCATALOG_DATABASE_ARG)
                .create());

        hCatOptions.addOption(OptionBuilder.withArgName("dir")
                .hasArg().withDescription("Override $HIVE_HOME")
                .withLongOpt(HIVE_HOME_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder.withArgName("hdir")
                .hasArg().withDescription("Override $HCAT_HOME")
                .withLongOpt(HCATALOG_HOME_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder.withArgName("partition-key")
                .hasArg()
                .withDescription("Sets the partition key to use when importing to hive")
                .withLongOpt(HIVE_PARTITION_KEY_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder.withArgName("partition-value")
                .hasArg()
                .withDescription("Sets the partition value to use when importing "
                        + "to hive")
                .withLongOpt(HIVE_PARTITION_VALUE_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder
                .hasArg()
                .withDescription("Override mapping for specific column to hive"
                        + " types.")
                .withLongOpt(MAP_COLUMN_HIVE)
                .create());
        hCatOptions.addOption(OptionBuilder.withArgName("partition-key")
                .hasArg()
                .withDescription("Sets the partition keys to use when importing to hive")
                .withLongOpt(HCATCALOG_PARTITION_KEYS_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder.withArgName("partition-value")
                .hasArg()
                .withDescription("Sets the partition values to use when importing "
                        + "to hive")
                .withLongOpt(HCATALOG_PARTITION_VALUES_ARG)
                .create());
        return hCatOptions;
    }

    protected RelatedOptions getHCatImportOnlyOptions() {
        RelatedOptions hCatOptions = new RelatedOptions(
                "HCatalog import specific options");
        hCatOptions.addOption(OptionBuilder
                .withDescription("Create HCatalog before import")
                .withLongOpt(CREATE_HCATALOG_TABLE_ARG)
                .create());
        hCatOptions.addOption(OptionBuilder
                .withDescription("Drop and Create HCatalog before import")
                .withLongOpt(DROP_AND_CREATE_HCATALOG_TABLE)
                .create());
        hCatOptions.addOption(OptionBuilder
                .hasArg()
                .withDescription("HCatalog storage stanza for table creation")
                .withLongOpt(HCATALOG_STORAGE_STANZA_ARG)
                .create());
        return hCatOptions;
    }

    /**
     * @return options governing output format delimiters
     */
    protected RelatedOptions getOutputFormatOptions() {
        RelatedOptions formatOpts = new RelatedOptions(
                "Output line formatting arguments");
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the field separator character")
                .withLongOpt(FIELDS_TERMINATED_BY_ARG)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the end-of-line character")
                .withLongOpt(LINES_TERMINATED_BY_ARG)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets a field enclosing character")
                .withLongOpt(OPTIONALLY_ENCLOSED_BY_ARG)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets a required field enclosing character")
                .withLongOpt(ENCLOSED_BY_ARG)
                .create());
        formatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the escape character")
                .withLongOpt(ESCAPED_BY_ARG)
                .create());
        formatOpts.addOption(OptionBuilder
                .withDescription("Uses MySQL's default delimiter set: "
                        + "fields: ,  lines: \\n  escaped-by: \\  optionally-enclosed-by: '")
                .withLongOpt(MYSQL_DELIMITERS_ARG)
                .create());

        return formatOpts;
    }

    /**
     * @return options governing input format delimiters.
     */
    protected RelatedOptions getInputFormatOptions() {
        RelatedOptions inputFormatOpts =
                new RelatedOptions("Input parsing arguments");
        inputFormatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the input field separator")
                .withLongOpt(INPUT_FIELDS_TERMINATED_BY_ARG)
                .create());
        inputFormatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the input end-of-line char")
                .withLongOpt(INPUT_LINES_TERMINATED_BY_ARG)
                .create());
        inputFormatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets a field enclosing character")
                .withLongOpt(INPUT_OPTIONALLY_ENCLOSED_BY_ARG)
                .create());
        inputFormatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets a required field encloser")
                .withLongOpt(INPUT_ENCLOSED_BY_ARG)
                .create());
        inputFormatOpts.addOption(OptionBuilder.withArgName("char")
                .hasArg()
                .withDescription("Sets the input escape character")
                .withLongOpt(INPUT_ESCAPED_BY_ARG)
                .create());

        return inputFormatOpts;
    }
    protected RelatedOptions getHBaseOptions() {
        RelatedOptions hbaseOpts =
                new RelatedOptions("HBase arguments");
        hbaseOpts.addOption(OptionBuilder.withArgName("table")
                .hasArg()
                .withDescription("Import to <table> in HBase")
                .withLongOpt(HBASE_TABLE_ARG)
                .create());
        hbaseOpts.addOption(OptionBuilder.withArgName("family")
                .hasArg()
                .withDescription("Sets the target column family for the import")
                .withLongOpt(HBASE_COL_FAM_ARG)
                .create());
        hbaseOpts.addOption(OptionBuilder.withArgName("col")
                .hasArg()
                .withDescription("Specifies which input column to use as the row key")
                .withLongOpt(HBASE_ROW_KEY_ARG)
                .create());
        hbaseOpts.addOption(OptionBuilder
                .withDescription("Enables HBase bulk loading")
                .withLongOpt(HBASE_BULK_LOAD_ENABLED_ARG)
                .create());
        hbaseOpts.addOption(OptionBuilder
                .withDescription("If specified, create missing HBase tables")
                .withLongOpt(HBASE_CREATE_TABLE_ARG)
                .create());

        return hbaseOpts;
    }
    @SuppressWarnings("static-access")
    protected void addValidationOpts(RelatedOptions validationOptions) {
        validationOptions.addOption(OptionBuilder
                .withDescription("Validate the copy using the configured validator")
                .withLongOpt(VALIDATE_ARG)
                .create());
        validationOptions.addOption(OptionBuilder
                .withArgName(VALIDATOR_CLASS_ARG).hasArg()
                .withDescription("Fully qualified class name for the Validator")
                .withLongOpt(VALIDATOR_CLASS_ARG)
                .create());
        validationOptions.addOption(OptionBuilder
                .withArgName(VALIDATION_THRESHOLD_CLASS_ARG).hasArg()
                .withDescription("Fully qualified class name for ValidationThreshold")
                .withLongOpt(VALIDATION_THRESHOLD_CLASS_ARG)
                .create());
        validationOptions.addOption(OptionBuilder
                .withArgName(VALIDATION_FAILURE_HANDLER_CLASS_ARG).hasArg()
                .withDescription("Fully qualified class name for "
                        + "ValidationFailureHandler")
                .withLongOpt(VALIDATION_FAILURE_HANDLER_CLASS_ARG)
                .create());
    }


    /**
     * Apply common command-line to the state.
     */
    protected void applyCommonOptions(CommandLine in, Droop2Options out)
            throws InvalidOptionsException {

        // common options.
        if (in.hasOption(VERBOSE_ARG)) {
            // Immediately switch into DEBUG logging.
            out.setVerbose(true);
            LoggingUtils.setDebugLevel();
            LOG.debug("Enabled debug logging.");
        }

        if (in.hasOption(HELP_ARG)) {
            ToolOptions toolOpts = new ToolOptions();
            configureOptions(toolOpts);
            printHelp(toolOpts);
            throw new InvalidOptionsException("");
        }

        if (in.hasOption(CONNECT_STRING_ARG)) {
            out.setConnectString(in.getOptionValue(CONNECT_STRING_ARG));
        }

        if (in.hasOption(CONNECT_PARAM_FILE)) {
            File paramFile = new File(in.getOptionValue(CONNECT_PARAM_FILE));
            if (!paramFile.exists()) {
                throw new InvalidOptionsException(
                        "Specified connection parameter file not found: " + paramFile);
            }
            InputStream inStream = null;
            Properties connectionParams = new Properties();
            try {
                inStream = new FileInputStream(
                        new File(in.getOptionValue(CONNECT_PARAM_FILE)));
                connectionParams.load(inStream);
            } catch (IOException ex) {
                LOG.warn("Failed to load connection parameter file", ex);
                throw new InvalidOptionsException(
                        "Error while loading connection parameter file: "
                                + ex.getMessage());
            } finally {
                if (inStream != null) {
                    try {
                        inStream.close();
                    } catch (IOException ex) {
                        LOG.warn("Failed to close input stream", ex);
                    }
                }
            }
            LOG.debug("Loaded connection parameters: " + connectionParams);
            out.setConnectionParams(connectionParams);
        }

        if (in.hasOption(NULL_STRING)) {
            out.setNullStringValue(in.getOptionValue(NULL_STRING));
        }

        if (in.hasOption(INPUT_NULL_STRING)) {
            out.setInNullStringValue(in.getOptionValue(INPUT_NULL_STRING));
        }

        if (in.hasOption(NULL_NON_STRING)) {
            out.setNullNonStringValue(in.getOptionValue(NULL_NON_STRING));
        }

        if (in.hasOption(INPUT_NULL_NON_STRING)) {
            out.setInNullNonStringValue(in.getOptionValue(INPUT_NULL_NON_STRING));
        }

        if (in.hasOption(DRIVER_ARG)) {
            out.setDriverClassName(in.getOptionValue(DRIVER_ARG));
        }

        applyCredentialsOptions(in, out);
    }

    private void applyCredentialsOptions(CommandLine in, Droop2Options out)
            throws InvalidOptionsException {
        if (in.hasOption(USERNAME_ARG)) {
            out.setUsername(in.getOptionValue(USERNAME_ARG));
            if (null == out.getPassword()) {
                // Set password to empty if the username is set first,
                // to ensure that they're either both null or neither is.
                out.setPassword("");
            }
        }

        if (in.hasOption(PASSWORD_ARG)) {
            LOG.warn("Setting your password on the command-line is insecure. "
                    + "Consider using -" + PASSWORD_PROMPT_ARG + " instead.");
            out.setPassword(in.getOptionValue(PASSWORD_ARG));
        }

        if (in.hasOption(PASSWORD_PROMPT_ARG)) {
            out.setPasswordFromConsole();
        }

        if (in.hasOption(PASSWORD_PATH_ARG)) {
            if (in.hasOption(PASSWORD_ARG) || in.hasOption(PASSWORD_PROMPT_ARG)
                    || in.hasOption(PASSWORD_ALIAS_ARG)) {
                throw new InvalidOptionsException("Only one of password, password "
                        + "alias or path to a password file must be specified.");
            }

            try {
                out.setPasswordFilePath(in.getOptionValue(PASSWORD_PATH_ARG));
                // apply password from file into password in options
                out.setPassword(CredentialsUtil2.fetchPassword(out));
                // And allow the PasswordLoader to clean up any sensitive properties
                CredentialsUtil2.cleanUpSensitiveProperties(out.getConf());
            } catch (IOException ex) {
                LOG.warn("Failed to load password file", ex);
                throw (InvalidOptionsException)
                        new InvalidOptionsException("Error while loading password file: "
                                + ex.getMessage()).initCause(ex);
            }
        }
    }

    protected Class<?> getClassByName(String className)
            throws InvalidOptionsException {
        try {
            return Class.forName(className, true,
                    Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new InvalidOptionsException(e.getMessage());
        }
    }

    protected void validateCommonOptions(Droop2Options options)
            throws InvalidOptionsException {
        if (options.getConnectString() == null) {
            throw new InvalidOptionsException(
                    "Error: Required argument --connect is missing."
                            + HELP_STR);
        }
    }

    private boolean isSet(String option) {
        return org.apache.commons.lang.StringUtils.isNotBlank(option);
    }

    /**
     * Given an array of extra arguments (usually populated via
     * this.extraArguments), determine the offset of the first '--'
     * argument in the list. Return 'extra.length' if there is none.
     */
    protected int getDashPosition(String [] extra) {
        int dashPos = extra.length;
        for (int i = 0; i < extra.length; i++) {
            if (extra[i].equals("--")) {
                dashPos = i;
                break;
            }
        }

        return dashPos;
    }
}
