package org.tupelo_schneck.electric;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.TimeZone;

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

public class Options extends org.apache.commons.cli.Options {
    static {
        System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
        
        // The following allows you can access an https URL without having the certificate in the truststore 
        TrustManager[] trustAllCerts = new TrustManager[] { 
            new X509TrustManager() { 
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
                    return null; 
                } 
                public void checkClientTrusted( java.security.cert.X509Certificate[] certs, String authType) { } 
                public void checkServerTrusted( java.security.cert.X509Certificate[] certs, String authType) { } 
            } 
        }; 
        // Install the all-trusting trust manager 
        try { 
            SSLContext sc = SSLContext.getInstance("SSL"); 
            sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory()); 
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
        } catch (Exception e) { } 
    }

    public final TimeZone timeZone = TimeZone.getDefault();
    public final int timeZoneRawOffset = timeZone.getRawOffset() / 1000;

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

    public Options() {
        this.addOption("d","database-directory",true,"database directory (required)");
        this.addOption("p","port",true,"port served by datasource server (default 8081)");
        this.addOption("m","mtus",true,"number of MTUs (default 1)");
        this.addOption("g","gateway-url",true,"URL of TED 5000 gateway (default http://TED5000)");
        this.addOption("u","username",true,"username for password-protected TED gateway (will prompt for password; default none)");
        this.addOption("n","num-points",true,"target number of data points returned over the zoom region (default 1000)");
        this.addOption("x","max-points",true,"number of data points beyond which server will not go (default 5000)");
        this.addOption("l","server-log",true,"server request log filename; include string \"yyyy_mm_dd\" for automatic rollover; or use \"stderr\" (default no log)");
        this.addOption("i","import-interval",true,"seconds between imports of data (default 4)");
        this.addOption("o","import-overlap",true,"extra seconds imported each time for good measure (default 4)");        
        this.addOption("e","long-import-interval",true,"seconds between imports of whole hours (default 300)");
        @SuppressWarnings("static-access")
        Option voltageOpt = OptionBuilder.withLongOpt("voltage")
        .withDescription("whether to include voltage data (default no)")
        .hasOptionalArg().create("v"); 
        this.addOption(voltageOpt); //"v","voltage",true,"whether ('yes' or 'no') to include voltage data (default no)");
        this.addOption("k","volt-ampere-import-interval",true,"seconds between polls for kVA data (accepts decimal values; default 0 means no kVA data)");
        @SuppressWarnings("static-access")
        Option kvaThreadsOpt = OptionBuilder.withLongOpt("volt-ampere-threads")
        .withDescription("number of threads for polling kVA (default 0 means share short import thread)")
        .withArgName("arg")
        .hasArg().create(); 
        this.addOption(kvaThreadsOpt);
        this.addOption("h","help",false,"print this help text");
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
            if(cmd.hasOption("m")) {
                try {
                    mtus = Byte.parseByte(cmd.getOptionValue("m"));
                    if(mtus<=0 || mtus >4) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }
            }
            if(cmd.hasOption("p")) {
                try {
                    port = Integer.parseInt(cmd.getOptionValue("p"));
                    if(port<=0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("g")) {
                gatewayURL = cmd.getOptionValue("g");
            }
            if(cmd.hasOption("u")) {
                username = cmd.getOptionValue("u");
            }
            if(cmd.hasOption("n")) {
                try {
                    numDataPoints = Integer.parseInt(cmd.getOptionValue("n"));
                    if(numDataPoints<=0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("x")) {
                try {
                    maxDataPoints = Integer.parseInt(cmd.getOptionValue("x"));
                    if(maxDataPoints<=0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("i")) {
                try {
                    importInterval = Integer.parseInt(cmd.getOptionValue("i"));
                    if(importInterval<=0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("o")) {
                try {
                    importOverlap = Integer.parseInt(cmd.getOptionValue("o"));
                    if(importOverlap<0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("e")) {
                try {
                    longImportInterval = Integer.parseInt(cmd.getOptionValue("e"));
                    if(longImportInterval<=0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("l")) {
                serverLogFilename = cmd.getOptionValue("l");
            }
            if(cmd.hasOption("v")) {
                String val = cmd.getOptionValue("v");
                if(val==null) voltage = true;
                else {
                    val = val.toLowerCase();
                    if("yes".equals(val) || "true".equals(val)) {
                        voltage = true;
                    }
                    if("no".equals(val) || "false".equals(val)) {
                        voltage = false;
                    }
                }
            }
            if(cmd.hasOption("k")) {
                try {
                    double value = Double.parseDouble(cmd.getOptionValue("k"));
                    voltAmpereImportIntervalMS = (long)(1000 * value);
                    if(voltAmpereImportIntervalMS<0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
            }
            if(cmd.hasOption("volt-ampere-threads")) {
                try {
                    kvaThreads = Integer.parseInt(cmd.getOptionValue("volt-ampere-threads"));
                    if(kvaThreads<0) showUsageAndExit = true;
                }
                catch(NumberFormatException e) {
                    showUsageAndExit = true;
                }            
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
            
        if(showUsageAndExit) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar its-electric-*.jar [options]", 
                    "\noptions (-d is REQUIRED; other important options are -g, -m, -p):",
                    this,
                    "");
            return false;
        }
        else {
            if(username!=null) {
                System.err.print("Please enter password for username '" + username + "': ");
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                password = reader.readLine();
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
