package org.tupelo_schneck.electric;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;

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
    
    public Options() {
        this.addOption("p","port",true,"port served by datasource server (default 8081)");
        this.addOption("m","mtus",true,"number of MTUs (default 1)");
        this.addOption("g","gateway-url",true,"URL of TED 5000 gateway (default http://TED5000)");
        this.addOption("u","username",true,"username for password-protected TED gateway (will prompt for password; default none)");
        this.addOption("n","num-points",true,"target number of data points returned over the zoom region (default 1000)");
        this.addOption("x","max-points",true,"number of data points beyond which server will not go (default 5000)");
        this.addOption("l","server-log",true,"server request log filename; include string \"yyyy_mm_dd\" for automatic rollover; or use \"stderr\" (default no log)");
        this.addOption("i","import-interval",true,"seconds between imports of data (default 15)");
        this.addOption("o","import-overlap",true,"extra seconds imported each time for good measure (default 30)");        
        this.addOption("h","help",false,"print this help text");
    }

    /** Returns true if program should continue */
    public boolean parseOptions(final Main main, String[] args) throws IOException {
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
                main.mtus = Byte.parseByte(cmd.getOptionValue("m"));
                if(main.mtus<=0 || main.mtus >4) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }
        }
        if(cmd!=null && cmd.hasOption("p")) {
            try {
                main.port = Integer.parseInt(cmd.getOptionValue("p"));
                if(main.port<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("g")) {
            main.gatewayURL = cmd.getOptionValue("g");
        }
        if(cmd!=null && cmd.hasOption("u")) {
            main.username = cmd.getOptionValue("u");
        }
        if(cmd!=null && cmd.hasOption("n")) {
            try {
                main.numDataPoints = Integer.parseInt(cmd.getOptionValue("n"));
                if(main.numDataPoints<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("x")) {
            try {
                main.maxDataPoints = Integer.parseInt(cmd.getOptionValue("x"));
                if(main.maxDataPoints<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("i")) {
            try {
                main.importInterval = Integer.parseInt(cmd.getOptionValue("i"));
                if(main.importInterval<=0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("o")) {
            try {
                main.importOverlap = Integer.parseInt(cmd.getOptionValue("o"));
                if(main.importOverlap<0) showUsageAndExit = true;
            }
            catch(NumberFormatException e) {
                showUsageAndExit = true;
            }            
        }
        if(cmd!=null && cmd.hasOption("l")) {
            main.serverLogFilename = cmd.getOptionValue("l");
        }
        if(cmd!=null && cmd.hasOption("h")) {
            showUsageAndExit = true;
        }
        
        if(cmd!=null && cmd.getArgs().length==1) {
            main.dbFilename = cmd.getArgs()[0];
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
            if(main.username!=null) {
                System.err.print("Please enter password for username '" + main.username + "': ");
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                main.password = reader.readLine();
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(main.username, main.password.toCharArray());
                    }
                });
            }
            return true;
        }
    }
}
