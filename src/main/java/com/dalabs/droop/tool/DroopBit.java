package com.dalabs.droop.tool;

/**
 * Created by ronaldm on 12/31/2016.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import com.dalabs.droop.Droop2Options;
import com.dalabs.droop.Droop2Options.InvalidOptionsException;
import com.dalabs.droop.cli.DroopParser;
import com.dalabs.droop.cli.ToolOptions;
import com.dalabs.droop.tool.BitDesc;

public abstract class DroopBit {


    public static final Log LOG = LogFactory.getLog(DroopBit.class.getName());

    /**
     * Configuration key that specifies the set of ToolPlugin instances to load
     * before determining which DroopBit instance to load.
     */
    public static final String BIT_PLUGINS_KEY = "droop.tool.plugins";

    private static final Map<String, Class<? extends DroopBit>> BITS;
    private static final Map<String, String> DESCRIPTIONS;

    static {
        // All DroopBit instances should be registered here so that
        // they can be found internally.
        BITS = new TreeMap<String, Class<? extends DroopBit>>();
        DESCRIPTIONS = new TreeMap<String, String>();

        registerBit("list-tables", ListTablesBit.class,
                "List available tables in a schema");
        registerBit("list-schemas", ListSchemasBit.class,
                "List available schemas in Drill");
        registerBit("import", ImportBit.class,
                "Import a table from a database to HDFS");
        registerBit("import-all-tables", ImportAllTablesBit.class,
                "Import tables from a schema to HDFS");

        /*
        registerTool("codegen", CodeGenTool.class,
                "Generate code to interact with database records");
        registerTool("create-hive-table", CreateHiveTableTool.class,
                "Import a table definition into Hive");
        registerTool("eval", EvalSqlTool.class,
                "Evaluate a SQL statement and display the results");
        registerTool("export", ExportTool.class,
                "Export an HDFS directory to a database table");
        registerTool("import", ImportTool.class,
                "Import a table from a database to HDFS");
        registerTool("import-all-tables", ImportAllTablesTool.class,
                "Import tables from a database to HDFS");
        registerTool("import-mainframe", MainframeImportTool.class,
                "Import datasets from a mainframe server to HDFS");
        registerTool("help", HelpTool.class, "List available commands");
        registerTool("list-databases", ListDatabasesTool.class,
                "List available databases on a server");
        registerTool("list-tables", ListTablesTool.class,
                "List available tables in a database");
        registerTool("merge", MergeTool.class,
                "Merge results of incremental imports");
        registerTool("metastore", MetastoreTool.class,
                "Run a standalone Sqoop metastore");
        registerTool("job", JobTool.class,
                "Work with saved jobs");
        registerTool("version", VersionTool.class,
                "Display version information");
        */
    }

    /**
     * Add a bit to the available set of DroopBit instances.
     * @param bitName the name the user access the bit through.
     * @param cls the class providing the bit.
     * @param description a user-friendly description of the bit's function.
     */
    private static void registerBit(String bitName,
                                     Class<? extends DroopBit> cls, String description) {
        Class<? extends DroopBit> existing = BITS.get(bitName);
        if (null != existing) {
            // Already have a bit with this name. Refuse to start.
            throw new RuntimeException("A plugin is attempting to register a bit "
                    + "with name " + bitName + ", but this bit already exists ("
                    + existing.getName() + ")");
        }

        BITS.put(bitName, cls);
        DESCRIPTIONS.put(bitName, description);
    }

    /**
     * Add bit to available set of DroopBit instances using the BitDesc
     * struct as the sole argument.
     */
    private static void registerBit(BitDesc bitDescription) {
        registerBit(bitDescription.getName(), bitDescription.getBitClass(),
                bitDescription.getDesc());
    }


    /**
     * @return the list of available bits.
     */
    public static Set<String> getBitNames() {
        return BITS.keySet();
    }

    /**
     * @return the DroopBit instance with the provided name, or null
     * if no such bit exists.
     */
    public static DroopBit getBit(String bitName) {
        Class<? extends DroopBit> cls = BITS.get(bitName);
        try {
            if (null != cls) {
                DroopBit bit = cls.newInstance();
                bit.setBitName(bitName);
                return bit;
            }
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
            return null;
        }

        return null;
    }

    /**
     * @return the user-friendly description for a bit, or null if the bit
     * cannot be found.
     */
    public static String getBitDescription(String bitName) {
        return DESCRIPTIONS.get(bitName);
    }

    /** The name of the current bit. */
    private String bitName;

    /** Arguments that remained unparsed after parseArguments. */
    protected String [] extraArguments;

    public DroopBit() {
        this.bitName = "<" + this.getClass().getName() + ">";
    }

    public DroopBit(String name) {
        this.bitName = name;
    }

    public String getBitName() {
        return this.bitName;
    }

    protected void setBitName(String name) {
        this.bitName = name;
    }

    /**
     * Main body of code to run the tool.
     * @param options the Droop2Options configured via
     * configureOptions()/applyOptions().
     * @return an integer return code for external programs to consume. 0
     * represents success; nonzero means failure.
     */
    public abstract int run(Droop2Options options);

    /**
     * Configure the command-line arguments we expect to receive.
     * @param opts a ToolOptions that should be populated with sets of
     * RelatedOptions for the tool.
     */
    public void configureOptions(ToolOptions opts) {
        // Default implementation does nothing.
    }

    /**
     * Print the help message for this tool.
     * @param opts the configured tool options
     */
    public void printHelp(ToolOptions opts) {
        System.out.println("usage: droop " + getBitName()
                + " [GENERIC-ARGS] [BIT-ARGS]");
        System.out.println("");

        opts.printHelp();

        System.out.println("");
        System.out.println("Generic Hadoop command-line arguments:");
        System.out.println("(must preceed any bit-specific arguments)");
    }

    /** Generate the Droop2Options containing actual argument values from
     * the extracted CommandLine arguments.
     * @param in the CLI CommandLine that contain the user's set Options.
     * @param out the Droop2Options with all fields applied.
     * @throws InvalidOptionsException if there's a problem.
     */
    public void applyOptions(CommandLine in, Droop2Options out)
            throws InvalidOptionsException {
        // Default implementation does nothing.
    }

    /**
     * Validates options and ensures that any required options are
     * present and that any mutually-exclusive options are not selected.
     * @throws InvalidOptionsException if there's a problem.
     */
    public void validateOptions(Droop2Options options)
            throws InvalidOptionsException {
        // Default implementation does nothing.
    }

    /**
     * Configures a Droop2Options according to the specified arguments.
     * Reads a set of arguments and uses them to configure a Droop2Options
     * and its embedded configuration (i.e., through GenericOptionsParser.)
     * Stores any unparsed arguments in the extraArguments field.
     *
     * @param args the arguments to parse.
     * @param conf if non-null, set as the configuration for the returned
     * Droop2Options.
     * @param in a (perhaps partially-configured) Droop2Options. If null,
     * then a new Droop2Options will be used. If this has a null configuration
     * and conf is null, then a new Configuration will be inserted in this.
     * @param useGenericOptions if true, will also parse generic Hadoop
     * options into the Configuration.
     * @return a Droop2Options that is fully configured by a given tool.
     */
    public Droop2Options parseArguments(String [] args,
                                       Configuration conf, Droop2Options in, boolean useGenericOptions)
            throws ParseException, Droop2Options.InvalidOptionsException {
        Droop2Options out = in;

        if (null == out) {
            out = new Droop2Options();
        }

        if (null != conf) {
            // User specified a configuration; use it and override any conf
            // that may have been in the Droop2Options.
            out.setConf(conf);
        } else if (null == out.getConf()) {
            // User did not specify a configuration, but neither did the
            // Droop2Options. Fabricate a new one.
            out.setConf(new Configuration());
        }

        // This bit is the "active" bit; bind it in the Droop2Options.
        out.setActiveDroopBit(this);

        String [] bitArgs = args; // args after generic parser is done.
        /*
        if (useGenericOptions) {
            try {
                bitArgs = ConfigurationHelper.parseGenericOptions(
                        out.getConf(), args);
            } catch (IOException ioe) {
                ParseException pe = new ParseException(
                        "Could not parse generic arguments");
                pe.initCause(ioe);
                throw pe;
            }
        }
        */

        // Parse bit-specific arguments.
        ToolOptions toolOptions = new ToolOptions();
        configureOptions(toolOptions);
        CommandLineParser parser = new DroopParser();
        CommandLine cmdLine = parser.parse(toolOptions.merge(), bitArgs, true);
        applyOptions(cmdLine, out);
        this.extraArguments = cmdLine.getArgs();
        return out;
    }

    /**
     * Append 'extra' to extraArguments.
     */
    public void appendArgs(String [] extra) {
        int existingLen =
                (this.extraArguments == null) ? 0 : this.extraArguments.length;
        int newLen = (extra == null) ? 0 : extra.length;
        String [] newExtra = new String[existingLen + newLen];

        if (null != this.extraArguments) {
            System.arraycopy(this.extraArguments, 0, newExtra, 0, existingLen);
        }

        if (null != extra) {
            System.arraycopy(extra, 0, newExtra, existingLen, newLen);
        }

        this.extraArguments = newExtra;
    }

}
