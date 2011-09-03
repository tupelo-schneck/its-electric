package org.tupelo_schneck.electric;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

public class Options extends org.apache.commons.cli.Options {
    Log log = LogFactory.getLog(Options.class);
    
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

    public static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    public static final Pattern dateTimePattern = Pattern.compile("(\\d\\d\\d\\d)(?:-?+(\\d\\d?+)(?:-?+(\\d\\d?+)(?:[T\\s]++(\\d\\d??)(?>:?+(\\d\\d)(?>:?+(\\d\\d)(?>[.,]\\d*+)?+)?+)?+)?)?)?(Z|[+-](\\d\\d):?+(\\d\\d)?+)?+");
    
    private static int parseInt(String s,int def) {
        if(s==null) return def;
        return Integer.parseInt(s);
    }
    
    public static int timestampFromUserInput(String aInput, boolean isEnd, TimeZone aTimeZone) {
        String input = aInput.trim();
        Matcher matcher = dateTimePattern.matcher(input);
        if(!matcher.matches()) {
            return Integer.parseInt(input);
        }
        
        int year = parseInt(matcher.group(1),1);
        int month = parseInt(matcher.group(2),1);
        int day = parseInt(matcher.group(3),1);
        int hour = parseInt(matcher.group(4),0);
        int minute = parseInt(matcher.group(5),0);
        int second = parseInt(matcher.group(6),0);
        
        TimeZone thisTimeZone = aTimeZone;
        int timeZoneHours = 0;
        int timeZoneMinutes = 0;
        if(matcher.group(7)!=null) {
            thisTimeZone = GMT;
            char start = matcher.group(7).charAt(0);
            if(start=='+' || start=='-') {
                timeZoneHours = parseInt(matcher.group(8),0);
                timeZoneMinutes = parseInt(matcher.group(9),0);
                if(start=='-') {
                    timeZoneHours = -timeZoneHours;
                    timeZoneMinutes = -timeZoneMinutes;
                }
            }
        }
        
        GregorianCalendar calendar = new GregorianCalendar(thisTimeZone);
        calendar.set(year,month-1,day,hour,minute,second);
        if(isEnd) {
            boolean setting = matcher.group(2)==null;
            if(setting) calendar.set(GregorianCalendar.MONTH,calendar.getActualMaximum(GregorianCalendar.MONTH)); 
            setting = setting || matcher.group(3)==null;
            if(setting) calendar.set(GregorianCalendar.DAY_OF_MONTH,calendar.getActualMaximum(GregorianCalendar.DAY_OF_MONTH)); 
            setting = setting || matcher.group(4)==null;
            if(setting) calendar.set(GregorianCalendar.HOUR_OF_DAY,calendar.getActualMaximum(GregorianCalendar.HOUR_OF_DAY)); 
            setting = setting || matcher.group(5)==null;
            if(setting) calendar.set(GregorianCalendar.MINUTE,calendar.getActualMaximum(GregorianCalendar.MINUTE)); 
            setting = setting || matcher.group(6)==null;
            if(setting) calendar.set(GregorianCalendar.SECOND,calendar.getActualMaximum(GregorianCalendar.SECOND)); 
        }
        calendar.add(GregorianCalendar.HOUR_OF_DAY, -timeZoneHours);
        calendar.add(GregorianCalendar.MINUTE, -timeZoneMinutes);
        
        long time = calendar.getTimeInMillis() / 1000;
        if(time < 0) return 0;
        if(time > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int)time;
    }
    
    public String dbFilename = null;
    public String gatewayURL = "http://TED5000";
    public String username = null;
    public String password = null;
    public String serverLogFilename;
    public byte mtus = 1;
    public int importOverlap = 8;
    public int importInterval = 4; // seconds
    public int longImportInterval = 5*60;
    public int numDataPoints = 1000;
    public int maxDataPoints = 5000;
    public int port = 8081;
    public boolean voltage = false;
    public long voltAmpereImportIntervalMS = 0;
    public int kvaThreads = 0;
    public boolean record = true;
    public boolean serve = true;
    
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
        .hasArgs(3).withArgName("start> <end> <res").create();
        this.addOption(exportOpt);

        Option exportStyleOpt = OptionBuilder.withLongOpt("export-style")
        .withDescription("if 'timestamp', export data is one line per timestamp; otherwise one line per timestamp/mtu pair")
        .hasArg().create();
        this.addOption(exportStyleOpt);

        this.addOption("d","database-directory",true,"database directory (required)");
        this.addOption("p","port",true,"port served by datasource server (\"none\" same as --no-serve; default 8081)");
        this.addOption("m","mtus",true,"number of MTUs (default 1)");
        this.addOption("g","gateway-url",true,"URL of TED 5000 gateway (\"none\" same as --no-record; default http://TED5000)");
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
        
        this.addOption("h","help",false,"print this help text");
    }

    private boolean optionalBoolean(CommandLine cmd, String param, boolean defaultVal) {
        String val = cmd.getOptionValue(param);
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
    
    /** Returns true if program should continue */
    public boolean parseOptions(String[] args) throws IOException {
        // create the parser
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        boolean showUsageAndExit = false;
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
                if(cmd.hasOption("no-serve")) {
                    serve = !optionalBoolean(cmd,"no-serve",false);
                }
                if(cmd.hasOption("no-record")) {
                    record = !optionalBoolean(cmd,"no-record",false);
                }
                
                boolean recordChanged = false;
                boolean serveChanged = false;
                
                if(cmd.hasOption("time-zone")) {
                    String input = cmd.getOptionValue("time-zone");
                    Matcher m = Pattern.compile("([+-]\\d{2}):?+(\\d{2})").matcher(input);
                    if(m.matches()) {
                        input = "GMT" + m.group(1) + m.group(2);
                    }
                    recordTimeZone = TimeZone.getTimeZone(input);
                    serveTimeZone = TimeZone.getTimeZone(input);
                    recordChanged = true;
                    serveChanged = true;
                }

                if(cmd.hasOption("serve-time-zone")) {
                    String input = cmd.getOptionValue("serve-time-zone");
                    Matcher m = Pattern.compile("([+-]\\d{2}):?+(\\d{2})").matcher(input);
                    if(m.matches()) {
                        input = "GMT" + m.group(1) + m.group(2);
                    }
                    serveTimeZone = TimeZone.getTimeZone(input);
                    serveChanged = true;
                }

                if(cmd.hasOption("ted-no-dst")) {
                    if(optionalBoolean(cmd, "ted-no-dst", false)) {
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

                if(cmd.hasOption("record-time-zone")) {
                    String input = cmd.getOptionValue("record-time-zone");
                    Matcher m = Pattern.compile("([+-]\\d{2}):?+(\\d{2})").matcher(input);
                    if(m.matches()) {
                        input = "GMT" + m.group(1) + m.group(2);
                    }
                    recordTimeZone = TimeZone.getTimeZone(input);
                    recordChanged = true;
                }
                
                if(recordChanged) log.info("Record Time Zone: " + TimeZone.getCanonicalID(recordTimeZone.getID()) + ", " + recordTimeZone.getDisplayName());
                if(serveChanged) log.info("Serve Time Zone: " + TimeZone.getCanonicalID(serveTimeZone.getID()) + ", " + serveTimeZone.getDisplayName());

                if(cmd.hasOption("export")) {
                    serve = false;
                    record = false;
                    export = true;
                    String[] vals = cmd.getOptionValues("export");
                    startTime = timestampFromUserInput(vals[0],false,serveTimeZone);
                    endTime = timestampFromUserInput(vals[1],true,serveTimeZone);
                    resolution = Integer.parseInt(vals[2]);
                }
                if(cmd.hasOption("export-style")) {
                    exportByMTU = !cmd.getOptionValue("export-style").trim().toLowerCase().equals("timestamp");
                }
                
                if(cmd.hasOption("delete-until")) {
                    deleteUntil = timestampFromUserInput(cmd.getOptionValue("delete-until"),false,serveTimeZone);
                }
                
                if(cmd.hasOption("p")) {
                    String val = cmd.getOptionValue("p");
                    if(val.equals("none")) {
                        serve = false;
                    }
                    else {
                        port = Integer.parseInt(val);
                        if(port<=0) showUsageAndExit = true;
                    }
                }
                if(cmd.hasOption("g")) {
                    gatewayURL = cmd.getOptionValue("g");
                    if(gatewayURL.equals("none")) record = false;
                }
                if(cmd.hasOption("m")) {
                    mtus = Byte.parseByte(cmd.getOptionValue("m"));
                    if(mtus<=0 || mtus >4) showUsageAndExit = true;
                }
                if(cmd.hasOption("u")) {
                    username = cmd.getOptionValue("u");
                }
                if(cmd.hasOption("n")) {
                    numDataPoints = Integer.parseInt(cmd.getOptionValue("n"));
                    if(numDataPoints<=0) showUsageAndExit = true;
                }
                if(cmd.hasOption("x")) {
                    maxDataPoints = Integer.parseInt(cmd.getOptionValue("x"));
                    if(maxDataPoints<=0) showUsageAndExit = true;
                }
                if(cmd.hasOption("i")) {
                    importInterval = Integer.parseInt(cmd.getOptionValue("i"));
                    if(importInterval<0) showUsageAndExit = true;
                }
                if(cmd.hasOption("o")) {
                    importOverlap = Integer.parseInt(cmd.getOptionValue("o"));
                    if(importOverlap<0) showUsageAndExit = true;
                }
                if(cmd.hasOption("e")) {
                    longImportInterval = Integer.parseInt(cmd.getOptionValue("e"));
                    if(longImportInterval<0) showUsageAndExit = true;
                }
                if(cmd.hasOption("l")) {
                    serverLogFilename = cmd.getOptionValue("l");
                }
                if(cmd.hasOption("v")) {
                    voltage = optionalBoolean(cmd,"v",false);
                }
                if(cmd.hasOption("k")) {
                    double value = Double.parseDouble(cmd.getOptionValue("k"));
                    voltAmpereImportIntervalMS = (long)(1000 * value);
                    if(voltAmpereImportIntervalMS<0) showUsageAndExit = true;
                }
                if(cmd.hasOption("volt-ampere-threads")) {
                    kvaThreads = Integer.parseInt(cmd.getOptionValue("volt-ampere-threads"));
                    if(kvaThreads<0) showUsageAndExit = true;
                }
                if(cmd.hasOption("h")) {
                    showUsageAndExit = true;
                }
            
                if(cmd.hasOption("d")) {
                    dbFilename = cmd.getOptionValue("d");
                }
                else if(cmd.getArgs().length==1) {
                    dbFilename = cmd.getArgs()[0];
                }
                else {
                    showUsageAndExit = true;
                }
            }
            catch (NumberFormatException e) {
                showUsageAndExit = true;
            }
        }
          
        if(!serve && !record && !export) {
            showUsageAndExit = true;
        }
        
        if(showUsageAndExit) {
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
            return false;
        }
        else {
            if(record && username!=null) {
                System.err.print("Please enter password for username '" + username + "': ");
                password = Main.reader.readLine();
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
            }
            return true;
        }
    }
}
