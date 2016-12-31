/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dalabs.droop.tool;

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

import com.dalabs.droop.DroopOptions;
import com.dalabs.droop.DroopOptions.InvalidOptionsException;
import com.dalabs.droop.cli.DroopParser;
import com.dalabs.droop.cli.ToolOptions;
import com.dalabs.droop.tool.ToolDesc;

/**
 * Base class for Droop subprograms (e.g., DroopImport, DroopExport, etc.)
 * Allows subprograms to configure the arguments they accept and
 * provides an entry-point to the subprogram.
 */
public abstract class DroopTool {

  public static final Log LOG = LogFactory.getLog(DroopTool.class.getName());

  /**
   * Configuration key that specifies the set of ToolPlugin instances to load
   * before determining which DroopTool instance to load.
   */
  public static final String TOOL_PLUGINS_KEY = "sqoop.tool.plugins";

  private static final Map<String, Class<? extends DroopTool>> TOOLS;
  private static final Map<String, String> DESCRIPTIONS;

  static {
    // All DroopTool instances should be registered here so that
    // they can be found internally.
    TOOLS = new TreeMap<String, Class<? extends DroopTool>>();
    DESCRIPTIONS = new TreeMap<String, String>();

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
        "Run a standalone Droop metastore");
    registerTool("job", JobTool.class,
        "Work with saved jobs");
    registerTool("version", VersionTool.class,
        "Display version information");
    */
  }

  /**
   * Add a tool to the available set of DroopTool instances.
   * @param toolName the name the user access the tool through.
   * @param cls the class providing the tool.
   * @param description a user-friendly description of the tool's function.
   */
  private static void registerTool(String toolName,
      Class<? extends DroopTool> cls, String description) {
    Class<? extends DroopTool> existing = TOOLS.get(toolName);
    if (null != existing) {
      // Already have a tool with this name. Refuse to start.
      throw new RuntimeException("A plugin is attempting to register a tool "
          + "with name " + toolName + ", but this tool already exists ("
          + existing.getName() + ")");
    }

    TOOLS.put(toolName, cls);
    DESCRIPTIONS.put(toolName, description);
  }

  /**
   * Add tool to available set of DroopTool instances using the ToolDesc
   * struct as the sole argument.
   */
  private static void registerTool(ToolDesc toolDescription) {
    registerTool(toolDescription.getName(), toolDescription.getToolClass(),
        toolDescription.getDesc());
  }

  /**
   * Load plugins referenced in sqoop-site.xml or other config (e.g., tools.d/),
   * to allow external tool definitions.
   *
   * @return the Configuration used to load the plugins.
   */
  public static Configuration loadPlugins(Configuration conf) {
    conf = loadPluginsFromConfDir(conf);
    /*
    List<ToolPlugin> plugins =
        org.apache.sqoop.config.ConfigurationHelper.getInstances(
            conf, TOOL_PLUGINS_KEY, ToolPlugin.class);
    for (ToolPlugin plugin : plugins) {
      LOG.debug("Loading plugin: " + plugin.getClass().getName());
      List<ToolDesc> descriptions = plugin.getTools();
      for (ToolDesc desc : descriptions) {
        LOG.debug("  Adding tool: " + desc.getName()
            + " -> " + desc.getToolClass().getName());
        registerTool(desc);
      }
    }
    */

    return conf;
  }

  /**
   * If $SQOOP_CONF_DIR/tools.d/ exists and sqoop.tool.plugins is not set,
   * then we look through the files in that directory; they should contain
   * lines of the form 'plugin.class.name[=/path/to/containing.jar]'.
   *
   * <p>Put all plugin.class.names into the Configuration, and load any
   * specified jars into the ClassLoader.
   * </p>
   *
   * @param conf the current configuration to populate with class names.
   * @return conf again, after possibly populating sqoop.tool.plugins.
   */
  private static Configuration loadPluginsFromConfDir(Configuration conf) {
    if (conf.get(TOOL_PLUGINS_KEY) != null) {
      LOG.debug(TOOL_PLUGINS_KEY + " is set; ignoring tools.d");
      return conf;
    }

    String confDirName = System.getenv("SQOOP_CONF_DIR");
    if (null == confDirName) {
      LOG.warn("$SQOOP_CONF_DIR has not been set in the environment. "
          + "Cannot check for additional configuration.");
      return conf;
    }

    File confDir = new File(confDirName);
    File toolsDir = new File(confDir, "tools.d");

    if (toolsDir.exists() && toolsDir.isDirectory()) {
      // We have a tools.d subdirectory. Get the file list, sort it,
      // and process them in order.
      String [] fileNames = toolsDir.list();
      Arrays.sort(fileNames);

      for (String fileName : fileNames) {
        File f = new File(toolsDir, fileName);
        if (f.isFile()) {
          loadPluginsFromFile(conf, f);
        }
      }
    }

    // Set the classloader in this configuration so that it will use
    // the jars we just loaded in.
    conf.setClassLoader(Thread.currentThread().getContextClassLoader());
    return conf;
  }

  /**
   * Read the specified file and extract any ToolPlugin implementation
   * names from there.
   * @param conf the configuration to populate.
   * @param f the file containing the configuration data to add.
   */
  private static void loadPluginsFromFile(Configuration conf, File f) {
    Reader r = null;
    try {
      // The file format is actually Java properties-file syntax.
      r = new InputStreamReader(new FileInputStream(f));
      Properties props = new Properties();
      props.load(r);

      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        // Each key is a ToolPlugin class name.
        // Each value, if set, is the jar that contains it.
        String plugin = entry.getKey().toString();
        addPlugin(conf, plugin);

        String jarName = entry.getValue().toString();
        /*
        if (jarName.length() > 0) {
          ClassLoaderStack.addJarFile(jarName, plugin);
          LOG.debug("Added plugin " + plugin + " in jar " + jarName
              + " specified by " + f);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Added plugin " + plugin + " specified by " + f);
        }
        */
      }
    } catch (IOException ioe) {
      LOG.error("Error loading ToolPlugin information from file "
          + f + ": " + StringUtils.stringifyException(ioe));
    } finally {
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("Error closing file " + f + ": " + ioe);
        }
      }
    }
  }

  /**
   * Add the specified plugin class name to the configuration string
   * listing plugin classes.
   */
  private static void addPlugin(Configuration conf, String pluginName) {
    String existingPlugins = conf.get(TOOL_PLUGINS_KEY);
    String newPlugins = null;
    if (null == existingPlugins || existingPlugins.length() == 0) {
      newPlugins = pluginName;
    } else {
      newPlugins = existingPlugins + "," + pluginName;
    }

    conf.set(TOOL_PLUGINS_KEY, newPlugins);
  }

  /**
   * @return the list of available tools.
   */
  public static Set<String> getToolNames() {
    return TOOLS.keySet();
  }

  /**
   * @return the DroopTool instance with the provided name, or null
   * if no such tool exists.
   */
  public static DroopTool getTool(String toolName) {
    Class<? extends DroopTool> cls = TOOLS.get(toolName);
    try {
      if (null != cls) {
        DroopTool tool = cls.newInstance();
        tool.setToolName(toolName);
        return tool;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      return null;
    }

    return null;
  }

  /**
   * @return the user-friendly description for a tool, or null if the tool
   * cannot be found.
   */
  public static String getToolDescription(String toolName) {
    return DESCRIPTIONS.get(toolName);
  }

  /** The name of the current tool. */
  private String toolName;

  /** Arguments that remained unparsed after parseArguments. */
  protected String [] extraArguments;

  public DroopTool() {
    this.toolName = "<" + this.getClass().getName() + ">";
  }

  public DroopTool(String name) {
    this.toolName = name;
  }

  public String getToolName() {
    return this.toolName;
  }

  protected void setToolName(String name) {
    this.toolName = name;
  }

  /**
   * Main body of code to run the tool.
   * @param options the DroopOptions configured via
   * configureOptions()/applyOptions().
   * @return an integer return code for external programs to consume. 0
   * represents success; nonzero means failure.
   */
  public abstract int run(DroopOptions options);

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
    System.out.println("usage: sqoop " + getToolName()
        + " [GENERIC-ARGS] [TOOL-ARGS]");
    System.out.println("");

    opts.printHelp();

    System.out.println("");
    System.out.println("Generic Hadoop command-line arguments:");
    System.out.println("(must preceed any tool-specific arguments)");
    // ToolRunner.printGenericCommandUsage(System.out);
  }

  /** Generate the DroopOptions containing actual argument values from
   * the extracted CommandLine arguments.
   * @param in the CLI CommandLine that contain the user's set Options.
   * @param out the DroopOptions with all fields applied.
   * @throws InvalidOptionsException if there's a problem.
   */
  public void applyOptions(CommandLine in, DroopOptions out)
      throws InvalidOptionsException {
    // Default implementation does nothing.
  }

  /**
   * Validates options and ensures that any required options are
   * present and that any mutually-exclusive options are not selected.
   * @throws InvalidOptionsException if there's a problem.
   */
  public void validateOptions(DroopOptions options)
      throws InvalidOptionsException {
    // Default implementation does nothing.
  }

  /**
   * Configures a DroopOptions according to the specified arguments.
   * Reads a set of arguments and uses them to configure a DroopOptions
   * and its embedded configuration (i.e., through GenericOptionsParser.)
   * Stores any unparsed arguments in the extraArguments field.
   *
   * @param args the arguments to parse.
   * @param conf if non-null, set as the configuration for the returned
   * DroopOptions.
   * @param in a (perhaps partially-configured) DroopOptions. If null,
   * then a new DroopOptions will be used. If this has a null configuration
   * and conf is null, then a new Configuration will be inserted in this.
   * @param useGenericOptions if true, will also parse generic Hadoop
   * options into the Configuration.
   * @return a DroopOptions that is fully configured by a given tool.
   */
  public DroopOptions parseArguments(String [] args,
      Configuration conf, DroopOptions in, boolean useGenericOptions)
      throws ParseException, DroopOptions.InvalidOptionsException {
    DroopOptions out = in;

    if (null == out) {
      out = new DroopOptions();
    }

    if (null != conf) {
      // User specified a configuration; use it and override any conf
      // that may have been in the DroopOptions.
      out.setConf(conf);
    } else if (null == out.getConf()) {
      // User did not specify a configuration, but neither did the
      // DroopOptions. Fabricate a new one.
      out.setConf(new Configuration());
    }

    // This tool is the "active" tool; bind it in the DroopOptions.
    out.setActiveDroopTool(this);

    String [] toolArgs = args; // args after generic parser is done.
    if (useGenericOptions) {
      /*
      try {
        toolArgs = ConfigurationHelper.parseGenericOptions(
            out.getConf(), args);
      } catch (IOException ioe) {
        ParseException pe = new ParseException(
            "Could not parse generic arguments");
        pe.initCause(ioe);
        throw pe;
      }
      */
    }

    // Parse tool-specific arguments.
    ToolOptions toolOptions = new ToolOptions();
    configureOptions(toolOptions);
    CommandLineParser parser = new DroopParser();
    CommandLine cmdLine = parser.parse(toolOptions.merge(), toolArgs, true);
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
  @Override
  public String toString() {
    return getToolName();
  }
}

