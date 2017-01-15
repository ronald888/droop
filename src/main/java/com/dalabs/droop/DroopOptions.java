package com.dalabs.droop;

/**
 * Created by ronaldm on 12/31/2016.
 */
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.dalabs.droop.util.StoredAsProperty;
import com.dalabs.droop.tool.DroopTool;

public class DroopOptions implements Cloneable {

    public static final Log LOG = LogFactory.getLog(DroopOptions.class.getName());

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
    @StoredAsProperty("db.connect.string") private String connectString;
    @StoredAsProperty("db.table") private String tableName;
    private String [] columns; // Array stored as db.column.list.
    @StoredAsProperty("db.username") private String username;

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
    // with 400 permissions so its only readable by user executing the tool
    @StoredAsProperty("db.password.file") private String passwordFilePath;
    @StoredAsProperty("null.string") private String nullStringValue;
    @StoredAsProperty("input.null.string") private String inNullStringValue;
    @StoredAsProperty("null.non-string") private String nullNonStringValue;
    @StoredAsProperty("input.null.non-string")
    private String inNullNonStringValue;

    @StoredAsProperty("db.where.clause") private String whereClause;
    @StoredAsProperty("db.query") private String sqlQuery;
    @StoredAsProperty("db.query.boundary") private String boundaryQuery;
    @StoredAsProperty("jdbc.driver.class") private String driverClassName;
    @StoredAsProperty("hdfs.warehouse.dir") private String warehouseDir;
    @StoredAsProperty("hdfs.target.dir") private String targetDir;
    @StoredAsProperty("hdfs.append.dir") private boolean append;
    @StoredAsProperty("hdfs.delete-target.dir") private boolean delete;
    @StoredAsProperty("hdfs.file.format") private FileLayout layout;

    // the connection manager fully qualified class name
    @StoredAsProperty("connection.manager") private String connManagerClassName;

    private DroopTool activeDroopTool;

    /** @return the DroopTool that is operating this session. */
    public DroopTool getActiveDroopTool() {
        return activeDroopTool;
    }

    public void setActiveDroopTool(DroopTool tool) {
        activeDroopTool = tool;
    }

    private Configuration conf;
    private String toolName;

    private String [] extraArgs;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration config) {
        this.conf = config;
    }

    public String getToolName() {
        return this.toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public DroopOptions() {
        initDefaults(null);
    }

    public DroopOptions(Configuration conf) {
        initDefaults(conf);
    }

    /**
     * Alternate DroopOptions interface used mostly for unit testing.
     * @param connect JDBC connect string to use
     * @param table Table to read
     */
    public DroopOptions(final String connect, final String table) {
        initDefaults(null);

        this.connectString = connect;
        this.tableName = table;
    }

    private void initDefaults(Configuration baseConfiguration) {
        // first, set the true defaults if nothing else happens.
        // default action is to run the full pipeline.

        // We do not want to be verbose too much if not explicitly needed
        this.verbose = false;
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
            DroopOptions other = (DroopOptions) super.clone();
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