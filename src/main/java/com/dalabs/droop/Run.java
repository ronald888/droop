package com.dalabs.droop;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ronaldm on 12/30/2016.
 */
public class Run {
    static final Logger LOG = LoggerFactory.getLogger(Run.class);

    public static void main(String[] args) throws IOException, Exception {
        final String message = "USAGE:\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n"
                + "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n";
        Preconditions.checkArgument(args.length > 1, message);

        switch (args[0]) {
            case "producer":
                break;
            case "consumer":
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
        System.exit(0);
    }
}
