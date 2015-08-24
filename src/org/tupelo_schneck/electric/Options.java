/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2015 Robert R. Tupelo-Schneck <schneck@gmail.com>
http://tupelo-schneck.org/its-electric

"it's electric" is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

"it's electric" is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with "it's electric", as legal/COPYING-agpl.txt.
If not, see <http://www.gnu.org/licenses/>.
 */

package org.tupelo_schneck.electric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tupelo_schneck.electric.current_cost.CurrentCostImporter;

import com.ibm.icu.util.TimeZone;

public class Options extends org.apache.commons.cli.Options {
    private static final String ITS_ELECTRIC_PROPERTIES = "its-electric.properties";

    Log log = LogFactory.getLog(Options.class);

    public static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    static {
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");

        // The following allows you can access an https URL without having the certificate in the truststore 
        TrustManager[] trustAllCerts = new TrustManager[] { 
                new X509TrustManager() { 
                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
                        return null; 
                    } 
                    @Override
                    public void checkClientTrusted( java.security.cert.X509Certificate[] certs, String authType) { } 
                    @Override
                    public void checkServerTrusted( java.security.cert.X509Certificate[] certs, String authType) { } 
                } 
        }; 
        // Install the all-trusting trust manager 
        try { 
            SSLContext sc = SSLContext.getInstance("SSL"); 
            sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory()); 
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
        } catch (Exception e) { } 
    }

    public String device = null;
    
    public String dbFilename = null;
    public String gatewayURL = "http://TED5000";
    public String username = null;
    public String password = null;
    public String serverLogFilename;
    public byte mtus = 1;
    public byte spyders = 0;
    public int importOverlap = 8;
    public int importInterval = 4; // seconds
    public int longImportInterval = 5*60;
    public int numDataPoints = 1000;
    public int maxDataPoints = 5000;
    public int port = 8081;
    public boolean voltage = false;
    public long voltAmpereImportIntervalMS = 0;
    public int kvaThreads = 0;

    public boolean tedOptions;

    public boolean record = true;
    public boolean serve = true;

    public boolean ccOptions;
    public String ccPortName;
    public int ccMaxSensor = 0;
    public boolean ccSumClamps = true;
    public int ccNumberOfClamps = 3;

    public TimeZone recordTimeZone = TimeZone.getDefault();
    public TimeZone serveTimeZone = TimeZone.getDefault();

    public boolean export = false;
    public int startTime;
    public int endTime;
    public int resolution;
    public boolean exportByMTU = true;

    public int deleteUntil;

    @SuppressWarnings("static-access")
    public Options() {
        Option optionFile=OptionBuilder.withLongOpt("config-file")
                .withDescription("file to read config options from")
                .hasArg().withArgName("arg").create();
        this.addOption(optionFile);
        
        Option deviceOpt=OptionBuilder.withLongOpt("device")
                .withDescription("device (ted-5000, ted-pro, current-cost; default ted-5000)")
                .hasArg().withArgName("arg").create();
        this.addOption(deviceOpt);
        
        Option noServeOpt = OptionBuilder.withLongOpt("no-serve")
                .withDescription("if present, do not serve Google Visualization data")
                .hasOptionalArg().withArgName(null).create(); 
        this.addOption(noServeOpt);

        Option noRecordOpt = OptionBuilder.withLongOpt("no-record")
                .withDescription("if present, do not record data from TED")
                .hasOptionalArg().withArgName(null).create(); 
        this.addOption(noRecordOpt);

        Option exportOpt = OptionBuilder.withLongOpt("export")
                .withDescription("export CSV for existing data from <start> to <end> of resolution <res>; implies --no-serve --no-record")
                .hasArgs(3).withArgName("start> <end> <res").withValueSeparator(' ').create();
        this.addOption(exportOpt);

        Option exportStyleOpt = OptionBuilder.withLongOpt("export-style")
                .withDescription("if 'timestamp', export data is one line per timestamp; otherwise one line per timestamp/mtu pair")
                .hasArg().create();
        this.addOption(exportStyleOpt);

        this.addOption("d","database-directory",true,"database directory (required)");
        this.addOption("p","port",true,"port served by datasource server (\"none\" same as --no-serve; default 8081)");
        this.addOption("m","mtus",true,"number of MTUs (default 1)");
        this.addOption("s","spyders",true,"number of Spyder CTs (default 0)");
        this.addOption("g","gateway-url",true,"URL of TED gateway (\"none\" same as --no-record; default http://TED5000)");
        this.addOption("u","username",true,"username for password-protected TED gateway (will prompt for password; default none)");
        this.addOption("n","num-points",true,"target number of data points returned over the zoom region (default 1000)");
        this.addOption("x","max-points",true,"number of data points beyond which server will not go (default 5000)");
        this.addOption("l","server-log",true,"server request log filename; include string \"yyyy_mm_dd\" for automatic rollover; or use \"stderr\" (default no log)");
        this.addOption("i","import-interval",true,"seconds between imports of data, or 0 for only hour-long imports (default 4)");
        this.addOption("o","import-overlap",true,"extra seconds imported each time for good measure (default 8)");        
        this.addOption("e","long-import-interval",true,"seconds between imports of whole hours, or 0 for only short imports (default 300)");

        Option voltageOpt = OptionBuilder.withLongOpt("voltage")
                .withDescription("whether to include voltage data (default no)")
                .hasOptionalArg().withArgName(null).create("v"); 
        this.addOption(voltageOpt);

        Option kvaOpt = OptionBuilder.withLongOpt("volt-ampere-import-interval")
                .withDescription("seconds between polls for kVA data (accepts decimal values; default 0 means no kVA data)")
                .withArgName(null) // save space in help text
                .hasArg().create("k"); 
        this.addOption(kvaOpt);

        Option kvaThreadsOpt = OptionBuilder.withLongOpt("volt-ampere-threads")
                .withDescription("number of threads for polling kVA (default 0 means share short import thread)")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(kvaThreadsOpt);

        Option timeZoneOpt = OptionBuilder.withLongOpt("record-time-zone")
                .withDescription("time zone for TED Gateway as ISO8601 offset or tz/zoneinfo name (default use time zone of its-electric install)")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(timeZoneOpt);

        timeZoneOpt = OptionBuilder.withLongOpt("serve-time-zone")
                .withDescription("time zone for data service output as ISO8601 offset or tz/zoneinfo name (default use time zone of its-electric install)")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(timeZoneOpt);

        timeZoneOpt = OptionBuilder.withLongOpt("time-zone")
                .withDescription("convenience parameter combining serve-time-zone and record-time-zone")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(timeZoneOpt);

        timeZoneOpt = OptionBuilder.withLongOpt("ted-no-dst")
                .withDescription("convenience parameter; if set, record-time-zone is set to serve-time-zone with no DST")
                .hasOptionalArg().withArgName(null).create(); 
        this.addOption(timeZoneOpt);

        Option deleteUntilOpt = OptionBuilder.withLongOpt("delete-until")
                .withDescription("if present, delete all entries in database up to this time; confirmation required")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(deleteUntilOpt);

        Option ccListSerialPortsOpt = OptionBuilder.withLongOpt("cc-list-serial-ports")
                .withDescription("Current Cost: list all serial ports and exit")
                .create();
        this.addOption(ccListSerialPortsOpt);

        Option ccPortNameOpt = OptionBuilder.withLongOpt("cc-port-name")
                .withDescription("Current Cost: port name; required for Current Cost use")
                .withArgName("arg")
                .hasArg().create();
        this.addOption(ccPortNameOpt);

        Option ccMaxSensorOpt = OptionBuilder.withLongOpt("cc-max-sensor")
                .withDescription("Current Cost: highest sensor number recorded (default 0)")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(ccMaxSensorOpt);

        Option ccSeparateClampsOpt = OptionBuilder.withLongOpt("cc-separate-clamps")
                .withDescription("Current Cost: if present, have separate readings for each clamp")
                .hasOptionalArg().withArgName(null).create(); 
        this.addOption(ccSeparateClampsOpt);

        Option ccNumClampsOpt = OptionBuilder.withLongOpt("cc-num-clamps")
                .withDescription("Current Cost: if cc-separate-clamps, how many clamps to record for each sensor (default 3)")
                .withArgName("arg")
                .hasArg().create(); 
        this.addOption(ccNumClampsOpt);

        this.addOption("h","help",false,"print this help text");
    }

    private boolean optionalBoolean(OptionWrapper cmd, String long_param, String short_param, boolean defaultVal) {
        String val = cmd.getOptionValue(long_param, short_param);
        if(val==null) return !defaultVal;
        else {
            val = val.toLowerCase();
            if("yes".equals(val) || "true".equals(val)) {
                return true;
            }
            if("no".equals(val) || "false".equals(val)) {
                return false;
            }
            throw new NumberFormatException("expecting true/false or yes/no for " + val);
        }
    }

    private static class FileConfig {
        Properties props=new Properties();

        FileConfig(String fileName) throws IOException {
            props.load(new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"UTF-8")));
        }

        boolean hasOption(String longOpt,String shortOpt) {
            longOpt = (longOpt==null ? "" : longOpt);
            shortOpt = (shortOpt==null ? "" : shortOpt);
            return props.containsKey(longOpt) || props.containsKey(shortOpt);
        }

        String getOptionValue(String longOpt,String shortOpt) {
            if(!hasOption(longOpt,shortOpt)) return null;
            longOpt = (longOpt==null ? "" : longOpt);
            shortOpt = (shortOpt==null ? "" : shortOpt);

            String optionValue = null;
            optionValue = props.getProperty(longOpt);
            if(optionValue!=null && !optionValue.equals("")) return optionValue.trim();
            optionValue = props.getProperty(shortOpt);
            if(optionValue!=null && !optionValue.equals("")) return optionValue.trim();
            return  null;
        }
    }

    private class OptionWrapper {
        private final CommandLine cmd;
        private final FileConfig config;

        OptionWrapper(CommandLine cmd,FileConfig config) {
            this.cmd = cmd;
            this.config = config;
        }

        private boolean cmdHasOption(String longOpt, String shortOpt) {
            return (cmd.hasOption(longOpt) || cmd.hasOption(shortOpt));
        }

        private boolean configHasOption(String longOpt, String shortOpt) {
            return ((config!=null) ? config.hasOption(longOpt, shortOpt) : false);
        }

        boolean hasOption(String longOpt, String shortOpt) {
            longOpt = (longOpt==null ? "" : longOpt);
            shortOpt = (shortOpt==null ? "" : shortOpt);
            return cmdHasOption(longOpt, shortOpt) || configHasOption(longOpt, shortOpt);
        }

        String getOptionValue(String longOpt,String shortOpt) {
            String optionValue = null;

            longOpt = (longOpt==null ? "" : longOpt);
            shortOpt = (shortOpt==null? "" : shortOpt);

            Option option = !longOpt.equals("") ? Options.this.getOption(longOpt) : Options.this.getOption(shortOpt);

            if(cmdHasOption(longOpt, shortOpt)) {
                optionValue = cmd.getOptionValue(longOpt);
                if(optionValue==null)
                    optionValue = cmd.getOptionValue(shortOpt);
            }
            else if(configHasOption(longOpt,shortOpt)) {
                optionValue = config.getOptionValue(longOpt, shortOpt);

                if(optionValue==null && option.hasArg() && !option.hasOptionalArg()) {
                    throw new NumberFormatException(longOpt+"/"+shortOpt+" requires mandatory argument");
                }
                if(optionValue!=null && !option.hasArg() && !option.hasOptionalArg()) {
                    throw new NumberFormatException(longOpt+"/"+shortOpt+" does not have argument");
                }
            }
            return optionValue;
        }

        String[] getOptionValues(String longOpt, String shortOpt) {
            String[] optionValues = null;

            longOpt = (longOpt==null ? "" : longOpt);
            shortOpt = (shortOpt==null? "" : shortOpt);

            Option option = !longOpt.equals("") ? Options.this.getOption(longOpt) : Options.this.getOption(shortOpt);

            if(cmdHasOption(longOpt,shortOpt)) {
                optionValues = cmd.getOptionValues(longOpt);
                if(optionValues== null) optionValues = cmd.getOptionValues(shortOpt);
            }
            else if(configHasOption(longOpt,shortOpt)) {
                String optionValue = config.getOptionValue(longOpt, shortOpt);

                if(optionValue==null && (option.hasArgs() || option.hasArg())) {
                    throw new NumberFormatException(longOpt+"/"+shortOpt+" requires mandatory argument");
                }
                if(optionValue!=null && !option.hasArgs() && !option.hasOptionalArg() && !option.hasArg()) {
                    throw new NumberFormatException(longOpt+"/"+shortOpt+" does require an argument");
                }

                if(optionValue!=null) {
                    char argSeparator = option.getValueSeparator();
                    String argSeparatorRegex;
                    if(argSeparator==' ') argSeparatorRegex = "\\s++";
                    else argSeparatorRegex = Pattern.quote(String.valueOf(argSeparator));
                    optionValues = optionValue.split(argSeparatorRegex);
                }

                if(option.hasArgs() && (optionValues==null || optionValues.length!=option.getArgs())) {
                    throw new NumberFormatException(longOpt+"/"+shortOpt+" requires " + option.getArgs() + " mandatory arguments");
                }
            }
            return optionValues;
        }
    }

    /** Returns true if program should continue */
    public boolean parseOptions(String[] args) throws IOException {
        // create the parser
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        FileConfig config=null;
        OptionWrapper options=null;
        boolean showUsageAndExit = false;
        boolean listSerialPortsAndExit = false;
        boolean hasDbFilename = true;
        try {
            // parse the command line arguments
            cmd = parser.parse(this, args);
        }
        catch(ParseException exp) {
            // oops, something went wrong
            System.err.println(exp.getMessage());
            showUsageAndExit = true;
        }

        if(cmd==null) {
            showUsageAndExit = true;
        }
        else {
            try {
                if(cmd.hasOption("config-file")) {
                    String optionFileName = cmd.getOptionValue("config-file");

                    try {
                        config = new FileConfig(optionFileName);
                    }
                    catch(IOException exp) {
                        System.err.println(exp.getMessage());
                        showUsageAndExit = true;
                    }
                }

                options = new OptionWrapper(cmd, config);

                hasDbFilename = true;
                if(options.hasOption("database-directory","d")) {
                    dbFilename = options.getOptionValue("database-directory","d");
                }
                else if(cmd.getArgs().length==1) {
                    dbFilename = cmd.getArgs()[0];
                }
                else {
                    hasDbFilename = false;
                }

                if(config==null && hasDbFilename) {
                    File defaultConfigFile = new File(dbFilename,ITS_ELECTRIC_PROPERTIES);
                    if(defaultConfigFile.exists()) {
                        try {
                            config = new FileConfig(defaultConfigFile.getAbsolutePath());
                        }
                        catch(IOException exp) {
                            System.err.println(exp.getMessage());
                            showUsageAndExit = true;
                        }
                        options = new OptionWrapper(cmd,config);
                    }
                }

                if (options.hasOption("device", null)) {
                    device = options.getOptionValue("device", null);
                    
                    if ("ted-5000".equals(device)) {
                        tedOptions = true;
                    } else if ("ted-pro".equals(device)) {
                        tedOptions = true;
                    } else if ("current-cost".equals(device)) {
                        ccOptions = true;
                    } else {
                        showUsageAndExit = true;
                    }
                } else {
                    device = "ted-5000";
                }

                if(options.hasOption("no-serve", null)) {
                    serve = !optionalBoolean(options,"no-serve", null,false);
                }
                if(options.hasOption("no-record", null)) {
                    record = !optionalBoolean(options,"no-record", null,false);
                }

                boolean recordChanged = false;
                boolean serveChanged = false;

                if(options.hasOption("time-zone", null)) {
                    String input = options.getOptionValue("time-zone",null);
                    input = convertISO8601TimeZone(input);
                    recordTimeZone = TimeZone.getTimeZone(input);
                    serveTimeZone = TimeZone.getTimeZone(input);
                    recordChanged = true;
                    serveChanged = true;
                }

                if(options.hasOption("serve-time-zone", null)) {
                    String input = options.getOptionValue("serve-time-zone", null);
                    input = convertISO8601TimeZone(input);
                    serveTimeZone = TimeZone.getTimeZone(input);
                    serveChanged = true;
                }

                if(options.hasOption("ted-no-dst", null)) {
                    if(optionalBoolean(options, "ted-no-dst", null, false)) {
                        int offset = serveTimeZone.getRawOffset();
                        boolean negative = offset < 0;
                        if(negative) offset = -offset;
                        int hours = offset/3600000;
                        offset = offset - 3600000 * hours;
                        int minutes = offset/60000;
                        String input = "GMT" + (negative ? "-" : "+") + (hours<10 ? "0" : "") + hours + (minutes<10 ? "0" : "") + minutes;
                        recordTimeZone = TimeZone.getTimeZone(input);
                        recordChanged = true;
                    }
                }

                if(options.hasOption("record-time-zone", null)) {
                    String input = options.getOptionValue("record-time-zone", null);
                    input = convertISO8601TimeZone(input);
                    recordTimeZone = TimeZone.getTimeZone(input);
                    recordChanged = true;
                }

                if(recordChanged) log.info("Record Time Zone: " + TimeZone.getCanonicalID(recordTimeZone.getID()) + ", " + recordTimeZone.getDisplayName());
                if(serveChanged) log.info("Serve Time Zone: " + TimeZone.getCanonicalID(serveTimeZone.getID()) + ", " + serveTimeZone.getDisplayName());

                if(options.hasOption("export", null)) {
                    serve = false;
                    record = false;
                    export = true;
                    String[] vals = options.getOptionValues("export",null);
                    startTime = Util.timestampFromUserInput(vals[0],false,serveTimeZone);
                    endTime = Util.timestampFromUserInput(vals[1],true,serveTimeZone);
                    resolution = Integer.parseInt(vals[2]);
                }
                if(options.hasOption("export-style", null)) {
                    exportByMTU = !options.getOptionValue("export-style", null).trim().toLowerCase().equals("timestamp");
                }

                if(options.hasOption("delete-until", null)) {
                    deleteUntil = Util.timestampFromUserInput(options.getOptionValue("delete-until",null),false,serveTimeZone);
                }

                if(options.hasOption("port","p")) {
                    String val = options.getOptionValue("port","p");
                    if(val.equals("none")) {
                        serve = false;
                    }
                    else {
                        port = Integer.parseInt(val);
                        if(port<=0) showUsageAndExit = true;
                    }
                }
                if(options.hasOption("gateway-url","g")) {
                    tedOptions = true;
                    gatewayURL = options.getOptionValue("gateway-url","g");
                    if(gatewayURL.equals("none")) record = false;
                }
                if(options.hasOption("mtus","m")) {
                    tedOptions = true;
                    mtus = Byte.parseByte(options.getOptionValue("mtus","m"));
                    if(mtus<=0 || mtus >4) showUsageAndExit = true;
                }
                if(options.hasOption("spyders","s")) {
                    tedOptions = true;
                    spyders = Byte.parseByte(options.getOptionValue("spyders","s"));
                    if(spyders<=0 || spyders >32) showUsageAndExit = true;
                    if (spyders > 0 && !"ted-pro".equals(device)) showUsageAndExit = true;
                }
                if(options.hasOption("username","u")) {
                    tedOptions = true;
                    username = options.getOptionValue("username","u");
                }
                if(options.hasOption("num-points","n")) {
                    numDataPoints = Integer.parseInt(options.getOptionValue("num-points","n"));
                    if(numDataPoints<=0) showUsageAndExit = true;
                }
                if(options.hasOption("max-points","x")) {
                    maxDataPoints = Integer.parseInt(options.getOptionValue("max-points","x"));
                    if(maxDataPoints<=0) showUsageAndExit = true;
                }
                if(options.hasOption("import-interval","i")) {
                    tedOptions = true;
                    importInterval = Integer.parseInt(options.getOptionValue("import-interval","i"));
                    if(importInterval<0) showUsageAndExit = true;
                }
                if(options.hasOption("import-overlap","o")) {
                    tedOptions = true;
                    importOverlap = Integer.parseInt(options.getOptionValue("import-overlap","o"));
                    if(importOverlap<0) showUsageAndExit = true;
                }
                if(options.hasOption("long-import-interval","e")) {
                    tedOptions = true;
                    longImportInterval = Integer.parseInt(options.getOptionValue("long-import-interval","e"));
                    if(longImportInterval<0) showUsageAndExit = true;
                }
                if(options.hasOption("server-log","l")) {
                    serverLogFilename = options.getOptionValue("server-log","l");
                }
                if(options.hasOption("voltage","v")) {
                    tedOptions = true;
                    voltage = optionalBoolean(options,"voltage","v",false);
                }
                if(options.hasOption("volt-ampere-import-interval","k")) {
                    tedOptions = true;
                    double value = Double.parseDouble(options.getOptionValue("volt-ampere-import-interval","k"));
                    voltAmpereImportIntervalMS = (long)(1000 * value);
                    if(voltAmpereImportIntervalMS<0) showUsageAndExit = true;
                }
                if(options.hasOption("volt-ampere-threads",null)) {
                    tedOptions = true;
                    kvaThreads = Integer.parseInt(options.getOptionValue("volt-ampere-threads",null));
                    if(kvaThreads<0) showUsageAndExit = true;
                }
                if(options.hasOption("cc-port-name",null)) {
                    ccOptions = true;
                    ccPortName = options.getOptionValue("cc-port-name",null);
                }
                if(options.hasOption("cc-max-sensor",null)) {
                    ccOptions = true;
                    ccMaxSensor = Integer.parseInt(options.getOptionValue("cc-max-sensor",null));
                }
                if(options.hasOption("cc-separate-clamps",null)) {
                    ccOptions = true;
                    ccSumClamps = !optionalBoolean(options,"cc-separate-clamps",null,false);
                }
                if(options.hasOption("cc-num-clamps",null)) {
                    ccOptions = true;
                    ccNumberOfClamps = Integer.parseInt(options.getOptionValue("cc-num-clamps",null));
                }
                if(options.hasOption("help","h")) {
                    showUsageAndExit = true;
                }

                if(options.hasOption("cc-list-serial-ports",null)) {
                    listSerialPortsAndExit = true;
                }
            }
            catch (NumberFormatException e) {
                System.err.println(e.getMessage()); 
                showUsageAndExit = true;
            }
        }

        if(!hasDbFilename && !listSerialPortsAndExit) {
            showUsageAndExit = true;
        }
        else if(!serve && !record && !export && deleteUntil==0 && !listSerialPortsAndExit) {
            showUsageAndExit = true;
        }

        if(ccOptions && tedOptions) {
            System.out.println("Cannot combine TED and Current Cost options.");
            showUsageAndExit = true;
        }
        else if (ccOptions && ccPortName==null) {
            System.out.println("Current Cost use requires --cc-port-name.");
            showUsageAndExit = true;
        }

        if(showUsageAndExit) {
            showUsage();
            return false;
        }
        else if(listSerialPortsAndExit) {
            CurrentCostImporter.printSerialPortNames();
            return false;
        }
        else {
            if(record && username!=null) {
                readAndProcessPassword();
            }
            confirmDeleteUntil();
            return true;
        }
    }

    private void showUsage() {
        StringBuilder header = new StringBuilder();
        header.append("\n");
        header.append("The \"it's electric\" Java program is designed to perform two simultaneous\n");
        header.append("activities:\n");
        header.append("(1) it records data from TED into a permanent database; and\n");
        header.append("(2) it serves data from the database in Google Visualization API format.\n");
        header.append("Additionally, the program can\n");
        header.append("(3) export data from the database in CSV format.\n");
        header.append("\n");
        header.append("To export data, use: java -jar its-electric-*.jar -d <database-directory>\n");
        header.append("                                      --export <start> <end> <resolution>\n");
        header.append("\n");
        header.append("You can specify to only record data using option --no-serve (e.g. for an\n");
        header.append("unattended setup) and to only serve data using option --no-record (e.g. with a\n");
        header.append("static copy of an its-electric database).\n");
        header.append("\n");
        header.append("Options -d (specifying the database directory) and -m (specifying the number of\n");
        header.append("MTUs) are important for both recording and serving.  Option -g (the TED Gateway\n");
        header.append("URL) and options -v and -k (which determine whether to record voltage and\n");
        header.append("volt-amperes) are important for recording.  Option -p (the port on which to\n");
        header.append("serve) is important for serving.  Other options are generally minor.\n");
        header.append("\n");
        header.append("Options (-d is REQUIRED):");

        HelpFormatter help = new HelpFormatter();
        PrintWriter writer = new PrintWriter(System.out);
        writer.println("usage: java -jar its-electric-*.jar [options]");
        writer.println(header.toString());
        help.printOptions(writer,80,this,0,0);
        writer.flush();
        writer.close();
    }

    private void readAndProcessPassword() throws IOException {
        System.err.print("Please enter password for username '" + username + "': ");
        password = Options.reader.readLine();
        Authenticator.setDefault(new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password.toCharArray());
            }
        });
    }

    private void confirmDeleteUntil() {
        if(deleteUntil > 1230000000) {
            boolean doit;
            System.err.print("Irrevocably delete all data up to " + Util.dateString(deleteUntil) + "? (yes/no [no]) ");
            try {
                String input = Options.reader.readLine();
                doit = input!=null && input.toLowerCase().trim().equals("yes");
            }
            catch(IOException e) {
                doit = false;
            }
            if(!doit) deleteUntil = 0;
        }
    }

    private String convertISO8601TimeZone(String input) {
        Matcher m = Pattern.compile("([+-]\\d{2}):?+(\\d{2})").matcher(input);
        if(m.matches()) {
            input = "GMT" + m.group(1) + m.group(2);
        }
        return input;
    }
}
