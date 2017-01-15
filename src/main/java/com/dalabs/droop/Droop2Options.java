package com.dalabs.droop;

/**
 * Created by ronaldm on 12/31/2016.
 */

import com.dalabs.droop.tool.DroopBit;
import com.dalabs.droop.util.StoredAsProperty;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import com.dalabs.droop.lib.DelimiterSet;

import static com.dalabs.droop.Droop.DROOP_RETHROW_PROPERTY;

public class Droop2Options implements Cloneable {

    public static final Log LOG = LogFactory.getLog(Droop2Options.class.getName());

    /** Selects in-HDFS destination file format. */
    public enum FileLayout {
        TextFile,
        SequenceFile,
        AvroDataFile,
        ParquetFile
    }
    /**
     * Thrown when invalid cmdline options are given.
     */
    @SuppressWarnings("serial")
    public static class InvalidOptionsException extends Exception {

        private String message;

        public InvalidOptionsException(final String msg) {
            this.message = msg;
        }

        public String getMessage() {
            return message;
        }

        public String toString() {
            return getMessage();
        }
    }

    @StoredAsProperty("verbose") private boolean verbose;

    // If this property is set, always throw an exception during a job, do not just
    // exit with status 1.
    @StoredAsProperty("droop.throwOnError") private boolean throwOnError;

    @StoredAsProperty("db.connect.string") private String connectString;
    @StoredAsProperty("db.schema") private String schemaName;
    @StoredAsProperty("db.input.schema") private String inputSchemaName;
    @StoredAsProperty("db.output.schema") private String outputSchemaName;
    @StoredAsProperty("db.table") private String tableName;
    private String [] columns; // Array stored as db.column.list.
    @StoredAsProperty("db.username") private String username;

    public String[] getColumns() {
        if (null == columns) {
            return null;
        } else {
            return Arrays.copyOf(columns, columns.length);
        }
    }

    public String getColumnNameCaseInsensitive(String col){
        if (null != columns) {
            for(String columnName : columns) {
                if(columnName.equalsIgnoreCase(col)) {
                    return columnName;
                }
            }
        }
        return null;
    }

    public void setColumns(String [] cols) {
        if (null == cols) {
            this.columns = null;
        } else {
            this.columns = Arrays.copyOf(cols, cols.length);
        }
    }


    /**
     * The DROOP_RETHROW_PROPERTY system property is considered to be set if it is set to
     * any kind of String value, i.e. it is not null.
     */
    // Type of DROOP_RETHROW_PROPERTY is String only to provide backward compatibility.
    public static boolean isDroopRethrowSystemPropertySet() {
        return (System.getProperty(DROOP_RETHROW_PROPERTY) != null);
    }

    /**
     * Given a string containing a single character or an escape sequence
     * representing a char, return that char itself.
     *
     * Normal literal characters return themselves: "x" -&gt; 'x', etc.
     * Strings containing a '\' followed by one of t, r, n, or b escape to the
     * usual character as seen in Java: "\n" -&gt; (newline), etc.
     *
     * Strings like "\0ooo" return the character specified by the octal sequence
     * 'ooo'. Strings like "\0xhhh" or "\0Xhhh" return the character specified by
     * the hex sequence 'hhh'.
     *
     * If the input string contains leading or trailing spaces, these are
     * ignored.
     */
    public static char toChar(String charish) throws InvalidOptionsException {
        if (null == charish || charish.length() == 0) {
            throw new InvalidOptionsException("Character argument expected."
                    + "\nTry --help for usage instructions.");
        }

        if (charish.startsWith("\\0x") || charish.startsWith("\\0X")) {
            if (charish.length() == 3) {
                throw new InvalidOptionsException(
                        "Base-16 value expected for character argument."
                                + "\nTry --help for usage instructions.");
            } else {
                String valStr = charish.substring(3);
                int val = Integer.parseInt(valStr, 16);
                return (char) val;
            }
        } else if (charish.startsWith("\\0")) {
            if (charish.equals("\\0")) {
                // it's just '\0', which we can take as shorthand for nul.
                return DelimiterSet.NULL_CHAR;
            } else {
                // it's an octal value.
                String valStr = charish.substring(2);
                int val = Integer.parseInt(valStr, 8);
                return (char) val;
            }
        } else if (charish.startsWith("\\")) {
            if (charish.length() == 1) {
                // it's just a '\'. Keep it literal.
                return '\\';
            } else if (charish.length() > 2) {
                // we don't have any 3+ char escape strings.
                throw new InvalidOptionsException(
                        "Cannot understand character argument: " + charish
                                + "\nTry --help for usage instructions.");
            } else {
                // this is some sort of normal 1-character escape sequence.
                char escapeWhat = charish.charAt(1);
                switch(escapeWhat) {
                    case 'b':
                        return '\b';
                    case 'n':
                        return '\n';
                    case 'r':
                        return '\r';
                    case 't':
                        return '\t';
                    case '\"':
                        return '\"';
                    case '\'':
                        return '\'';
                    case '\\':
                        return '\\';
                    default:
                        throw new InvalidOptionsException(
                                "Cannot understand character argument: " + charish
                                        + "\nTry --help for usage instructions.");
                }
            }
        } else {
            // it's a normal character.
            if (charish.length() > 1) {
                LOG.warn("Character argument " + charish + " has multiple characters; "
                        + "only the first will be used.");
            }

            return charish.charAt(0);
        }
    }

    public boolean getVerbose() {
        return verbose;
    }

    public void setVerbose(boolean beVerbose) {
        this.verbose = beVerbose;
    }


    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectStr) {
        this.connectString = connectStr;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schema) {
        this.schemaName = schema;
    }

    public String getInputSchemaName() {
        return inputSchemaName;
    }

    public void setInputSchemaName(String schema) {
        this.inputSchemaName = schema;
    }

    public String getOutputSchemaName() {
        return outputSchemaName;
    }

    public void setOutputSchemaName(String schema) {
        this.outputSchemaName = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String table) {
        this.tableName = table;
    }

    private Properties connectionParams; //Properties stored as db.connect.params
    public void setConnectionParams(Properties params) {
        connectionParams = new Properties();
        connectionParams.putAll(params);
    }

    public Properties getConnectionParams() {
        return connectionParams;
    }


    // May not be serialized, based on configuration.
    // db.require.password is used to determine whether 'some' password is
    // used. If so, it is stored as 'db.password'.
    private String password;

    // This represents path to a file on ${user.home} containing the password
    // with 400 permissions so its only readable by user executing the bit
    @StoredAsProperty("db.password.file") private String passwordFilePath;
    @StoredAsProperty("null.string") private String nullStringValue;
    @StoredAsProperty("input.null.string") private String inNullStringValue;
    @StoredAsProperty("null.non-string") private String nullNonStringValue;
    @StoredAsProperty("input.null.non-string")
    private String inNullNonStringValue;

    public void setNullStringValue(String nullString) {
        this.nullStringValue = nullString;
    }

    public String getNullStringValue() {
        return nullStringValue;
    }

    public void setInNullStringValue(String inNullString) {
        this.inNullStringValue = inNullString;
    }

    public String getInNullStringValue() {
        return inNullStringValue;
    }

    public void setNullNonStringValue(String nullNonString) {
        this.nullNonStringValue = nullNonString;
    }

    public String getNullNonStringValue() {
        return nullNonStringValue;
    }

    public void setInNullNonStringValue(String inNullNonString) {
        this.inNullNonStringValue = inNullNonString;
    }

    public String getInNullNonStringValue() {
        return inNullNonStringValue;
    }

    @StoredAsProperty("db.where.clause") private String whereClause;
    @StoredAsProperty("db.query") private String sqlQuery;

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlStatement) {
        this.sqlQuery = sqlStatement;
    }

    @StoredAsProperty("db.query.boundary") private String boundaryQuery;
    @StoredAsProperty("jdbc.driver.class") private String driverClassName;
    @StoredAsProperty("hdfs.warehouse.dir") private String warehouseDir;
    @StoredAsProperty("hdfs.target.dir") private String targetDir;
    @StoredAsProperty("hdfs.append.dir") private boolean append;
    @StoredAsProperty("hdfs.delete-target.dir") private boolean delete;
    @StoredAsProperty("hdfs.file.format") private FileLayout layout;

    // the connection manager fully qualified class name
    @StoredAsProperty("connection.manager") private String connManagerClassName;

    /**
     * @return The JDBC driver class name specified with --driver.
     */
    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClass) {
        this.driverClassName = driverClass;
    }

    /**
     * @return the base destination path for table uploads.
     */
    public String getWarehouseDir() {
        return warehouseDir;
    }

    public void setWarehouseDir(String warehouse) {
        this.warehouseDir = warehouse;
    }

    public String getTargetDir() {
        return this.targetDir;
    }

    public void setTargetDir(String dir) {
        this.targetDir = dir;
    }

    public void setAppendMode(boolean doAppend) {
        this.append = doAppend;
    }

    public boolean isAppendMode() {
        return this.append;
    }

    public void setDeleteMode(boolean doDelete) {
        this.delete = doDelete;
    }

    public boolean isDeleteMode() {
        return this.delete;
    }

    /**
     * @return the destination file format
     */
    public FileLayout getFileLayout() {
        return this.layout;
    }

    public void setFileLayout(FileLayout fileLayout) {
        this.layout = fileLayout;
    }

    private DroopBit activeDroopBit;

    /** @return the DroopBit that is operating this session. */
    public DroopBit getActiveDroopBit() {
        return activeDroopBit;
    }

    public void setActiveDroopBit(DroopBit bit) {
        activeDroopBit = bit;
    }

    private Configuration conf;
    private String bitName;

    private String [] extraArgs;

    /**
     * @return command-line arguments after a '-'.
     */
    public String [] getExtraArgs() {
        if (extraArgs == null) {
            return null;
        }

        String [] out = new String[extraArgs.length];
        for (int i = 0; i < extraArgs.length; i++) {
            out[i] = extraArgs[i];
        }
        return out;
    }

    public void setExtraArgs(String [] args) {
        if (null == args) {
            this.extraArgs = null;
            return;
        }

        this.extraArgs = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            this.extraArgs[i] = args[i];
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration config) {
        this.conf = config;
    }

    public String getBitName() {
        return this.bitName;
    }

    public void setBitName(String bitName) {
        this.bitName = bitName;
    }

    public Droop2Options() {
        initDefaults(null);
    }

    public Droop2Options(Configuration conf) {
        initDefaults(conf);
    }

    /**
     * Alternate DroopOptions interface used mostly for unit testing.
     * @param connect JDBC connect string to use
     * @param table Table to read
     */
    public Droop2Options(final String connect, final String table) {
        initDefaults(null);

        this.connectString = connect;
        this.tableName = table;
    }

    private void initDefaults(Configuration baseConfiguration) {
        // first, set the true defaults if nothing else happens.
        // default action is to run the full pipeline.

        // We do not want to be verbose too much if not explicitly needed
        this.verbose = false;

        //This default value is set intentionally according to SQOOP_RETHROW_PROPERTY system property
        //to support backward compatibility. Do not exchange it.
        this.throwOnError = isDroopRethrowSystemPropertySet();
        this.layout = FileLayout.ParquetFile;

    }

    public boolean isThrowOnError() {
        return throwOnError;
    }

    public void setThrowOnError(boolean throwOnError) {
        this.throwOnError = throwOnError;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String where) {
        this.whereClause = where;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String user) {
        this.username = user;
    }

    public String getPassword() {
        return password;
    }

    public String getPasswordFilePath() {
        return passwordFilePath;
    }

    public void setPasswordFilePath(String passwdFilePath) {
        this.passwordFilePath = passwdFilePath;
    }

    public void setPassword(String pass) {
        this.password = pass;
    }


    /**
     * Allow the user to enter his password on the console without printing
     * characters.
     * @return the password as a string
     */
    private String securePasswordEntry() {
        try {
            return new String(System.console().readPassword("Enter password: "));
        } catch (NullPointerException e) {
            LOG.error("It seems that you have launched a Droop metastore job via");
            LOG.error("Oozie with droop.metastore.client.record.password disabled.");
            LOG.error("But this configuration is not supported because Droop can't");
            LOG.error("prompt the user to enter the password while being executed");
            LOG.error("as Oozie tasks. Please enable droop.metastore.client.record");
            LOG.error(".password in droop-site.xml, or provide the password");
            LOG.error("explicitly using --password in the command tag of the Oozie");
            LOG.error("workflow file.");
            return null;
        }
    }

    /**
     * Set the password in this DroopOptions from the console without printing
     * characters.
     */
    public void setPasswordFromConsole() {
        this.password = securePasswordEntry();
    }

    protected void parseColumnMapping(String mapping,
                                      Properties output) {
        output.clear();

        String[] maps = mapping.split(",");
        for(String map : maps) {
            String[] details = map.split("=");
            if (details.length != 2) {
                throw new IllegalArgumentException("Malformed mapping.  "
                        + "Column mapping should be the form key=value[,key=value]*");
            }

            try {
                output.put(
                        URLDecoder.decode(details[0], "UTF-8"),
                        URLDecoder.decode(details[1], "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Encoding not supported. "
                        + "Column mapping should be UTF-8 encoding.");
            }
        }
    }

    private void writePasswordProperty(Properties props) {
        if (getPasswordFilePath() != null) { // short-circuit
            putProperty(props, "db.password.file", getPasswordFilePath());
            return;
        }

        if (this.password != null) {
            // Otherwise, if the user has set a password, we just record
            // a flag stating that the password will need to be reentered.
            putProperty(props, "db.require.password", "true");
        } else {
            // No password saved or required.
            putProperty(props, "db.require.password", "false");
        }
    }

    @Override
    public Object clone() {
        try {
            Droop2Options other = (Droop2Options) super.clone();
            if (null != columns) {
                other.columns = Arrays.copyOf(columns, columns.length);
            }

            if (null != conf) {
                other.conf = new Configuration(conf);
            }

            if (null != extraArgs) {
                other.extraArgs = Arrays.copyOf(extraArgs, extraArgs.length);
            }

            if (null != connectionParams) {
                other.setConnectionParams(this.connectionParams);
            }

            return other;
        } catch (CloneNotSupportedException cnse) {
            // Shouldn't happen.
            return null;
        }
    }

    /** Take a comma-delimited list of input and split the elements
     * into an output array. */
    private String [] listToArray(String strList) {
        return strList.split(",");
    }

    private String arrayToList(String [] array) {
        if (null == array) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String elem : array) {
            if (!first) {
                sb.append(",");
            }
            sb.append(elem);
            first = false;
        }

        return sb.toString();
    }

    /**
     * A put() method for Properties that is tolerent of 'null' values.
     * If a null value is specified, the property is unset.
     */
    private void putProperty(Properties props, String k, String v) {
        if (null == v) {
            props.remove(k);
        } else {
            props.setProperty(k, v);
        }
    }

    /**
     * Given a property prefix that denotes a set of numbered properties,
     * return an array containing all the properties.
     *
     * For instance, if prefix is "foo", then return properties "foo.0",
     * "foo.1", "foo.2", and so on as an array. If no such properties
     * exist, return 'defaults'.
     */
    private String [] getArgArrayProperty(Properties props, String prefix,
                                          String [] defaults) {
        int cur = 0;
        ArrayList<String> al = new ArrayList<String>();
        while (true) {
            String curProp = prefix + "." + cur;
            String curStr = props.getProperty(curProp, null);
            if (null == curStr) {
                break;
            }

            al.add(curStr);
            cur++;
        }

        if (cur == 0) {
            // Couldn't find an array here; return the defaults.
            return defaults;
        }

        return al.toArray(new String[0]);
    }

    private void setArgArrayProperties(Properties props, String prefix,
                                       String [] values) {
        if (null == values) {
            return;
        }

        for (int i = 0; i < values.length; i++) {
            putProperty(props, prefix + "." + i, values[i]);
        }
    }

    /**
     * This method encodes the property key values found in the provided
     * properties instance <tt>values</tt> into another properties instance
     * <tt>props</tt>. The specified <tt>prefix</tt> is used as a namespace
     * qualifier for keys when inserting. This allows easy introspection of the
     * property key values in <tt>props</tt> instance to later separate out all
     * the properties that belong to the <tt>values</tt> instance.
     * @param props the container properties instance
     * @param prefix the prefix for qualifying contained property keys.
     * @param values the contained properties instance, all of whose elements will
     *               be added to the container properties instance.
     *
     * @see #getPropertiesAsNetstedProperties(Properties, String)
     */
    private void setPropertiesAsNestedProperties(Properties props,
                                                 String prefix, Properties values) {
        String nestedPropertyPrefix = prefix + ".";
        if (null == values || values.size() == 0) {
            Iterator<String> it = props.stringPropertyNames().iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (name.startsWith(nestedPropertyPrefix)) {
                    props.remove(name);
                }
            }
        } else {
            Iterator<String> it = values.stringPropertyNames().iterator();
            while (it.hasNext()) {
                String name = it.next();
                putProperty(props,
                        nestedPropertyPrefix + name, values.getProperty(name));
            }
        }
    }

    /**
     * This method decodes the property key values found in the provided
     * properties instance <tt>props</tt> that have keys beginning with the
     * given prefix. Matching elements from this properties instance are modified
     * so that their prefix is dropped.
     * @param props the properties container
     * @param prefix the prefix qualifying properties that need to be removed
     * @return a new properties instance that contains all matching elements from
     * the container properties.
     */
    private Properties getPropertiesAsNetstedProperties(
            Properties props, String prefix) {
        Properties nestedProps = new Properties();
        String nestedPropertyPrefix = prefix + ".";
        int index = nestedPropertyPrefix.length();
        if (props != null && props.size() > 0) {
            Iterator<String> it = props.stringPropertyNames().iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (name.startsWith(nestedPropertyPrefix)){
                    String shortName = name.substring(index);
                    nestedProps.put(shortName, props.get(name));
                }
            }
        }
        return nestedProps;
    }

}
