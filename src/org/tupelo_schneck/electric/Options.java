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
    public int importOverlap = 4;
    public int importInterval = 4; // seconds
    public int longImportInterval = 5*60;
    public int numDataPoints = 1000;
    public int maxDataPoints = 5000;
    public int port = 8081;

    public Options() {
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

        if(cmd!=null && cmd.hasOption("m")) {
            try {
                mtus = Byte.parseByte(cmd.getOptionValue("m"));
                if(mtus<=0 || mtus >4) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }
        }
        if(cmd!=null && cmd.hasOption("p")) {
            try {
                port = Integer.parseInt(cmd.getOptionValue("p"));
                if(port<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("g")) {
            gatewayURL = cmd.getOptionValue("g");
        }
        if(cmd!=null && cmd.hasOption("u")) {
            username = cmd.getOptionValue("u");
        }
        if(cmd!=null && cmd.hasOption("n")) {
            try {
                numDataPoints = Integer.parseInt(cmd.getOptionValue("n"));
                if(numDataPoints<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("x")) {
            try {
                maxDataPoints = Integer.parseInt(cmd.getOptionValue("x"));
                if(maxDataPoints<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("i")) {
            try {
                importInterval = Integer.parseInt(cmd.getOptionValue("i"));
                if(importInterval<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("o")) {
            try {
                importOverlap = Integer.parseInt(cmd.getOptionValue("o"));
                if(importOverlap<0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("e")) {
            try {
                longImportInterval = Integer.parseInt(cmd.getOptionValue("i"));
                if(longImportInterval<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("l")) {
            serverLogFilename = cmd.getOptionValue("l");
        }
        if(cmd!=null && cmd.hasOption("h")) {
            showUsageAndExit = true;
        }
        
        if(cmd!=null && cmd.getArgs().length==1) {
            dbFilename = cmd.getArgs()[0];
        }
        else {
            showUsageAndExit = true;
        }
        
        if(showUsageAndExit) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar its-electric-*.jar [options] database-directory", 
                    "\noptions (most important are -g, -m, -p):",
                    this,
                    "\nThe specified database-directory (REQUIRED) is the location of the database.");
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
