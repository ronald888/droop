package com.dalabs.droop;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.dalabs.droop.tool.DroopBit;
import com.dalabs.droop.util.OptionsFileUtil;

/**
 * Created by ronaldm on 12/30/2016.
 */
public class Droop {
    static final Logger LOG = LoggerFactory.getLogger(Droop.class);

    /**
     * If this System property is set, always throw an exception, do not just
     * exit with status 1.
     */
    public static final String DROOP_RETHROW_PROPERTY = "droop.throwOnError";

    public static final String DROOP_OPTIONS_FILE_SPECIFIER = "--options-file";

    private DroopBit bit;
    private Droop2Options options;

    public Droop(DroopBit bit) {
        this(bit, new Droop2Options());
    }

    public Droop(DroopBit bit, Droop2Options opts) {
        LOG.info("Running Droop version: ");
        /**
         * TODO: set options conf here
         */
        this.options = opts;
        this.bit = bit;
    }

    public DroopBit getBit() {
        return this.bit;
    }

    public Droop2Options getOptions() {
        return this.options;
    }

    public static int runDroop(Droop droop, String [] args) {

        Droop2Options options = droop.getOptions();
        DroopBit bit = droop.getBit();

        try {
            options = bit.parseArguments(args, null, options, false);
            bit.validateOptions(options);
        } catch (Exception e) {
            LOG.debug(e.getMessage(), e);
            System.err.println(e.getMessage());
            return 1;
        }

        return bit.run(options);
    }

    public static int runBit(String [] args) {
        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception ex) {
            LOG.error("Error while expanding arguments", ex);
            System.err.println(ex.getMessage());
            System.err.println("Try 'droop help' for usage.");
            return 1;
        }
        for (String arg1 : expandedArgs) {
            System.out.println(arg1);
        }
        String bitName = expandedArgs[0];
        DroopBit bit = DroopBit.getBit(bitName);
        if (null == bit) {
            System.err.println("No such droop bit: " + bitName
                    + ". See 'droop help'.");
            return 1;
        } else {
            Droop droop = new Droop(bit);
            return runDroop(droop,
                    Arrays.copyOfRange(expandedArgs, 1, expandedArgs.length));
        }
    }
    public static void main(String[] args) {
        int ret = runBit(args);

        System.exit(ret);
    }
}
