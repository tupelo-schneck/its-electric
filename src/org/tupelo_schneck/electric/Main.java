/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2010 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class Main {
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
    
    private Log log = LogFactory.getLog(Main.class);
    
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };
    public static final String[] durationStrings = new String[] {
        "1\"","4\"","15\"","1'","4'","15'","1h","3h","8h","1d"
    };

    public Environment environment;
    public TimeSeriesDatabase[] databases = new TimeSeriesDatabase[durations.length];

    public String gatewayURL = "http://TED5000";
    public String username = "";
    public String password = "";
    public String serverLogFilename;
    public byte mtus = 1;
    public int importOverlap = 30;
    public int importInterval = 15; // seconds
    public int numDataPoints = 1000;
    public int maxDataPoints = 5000;
    public int port = 8081;

    public int adjustment; // system time minus gateway time
    
    public int minimum;
    public volatile int maximum;
    
    public static final int timeZoneOffset = TimeZone.getDefault().getRawOffset() / 1000;

    public int[][] maxForMTU;

    // for i > 0, start[i][mtu] is the next entry that database i will get;
    // sum[i][mtu] and count[i][mtu] are accumulated to find the average.
    public int[][] start;
    public int[][] sum;
    public int[][] count;

    public boolean caughtUp;

    public void put(int timestamp, byte mtu, int power) throws DatabaseException {
        databases[0].put(timestamp,mtu,power);
        maxForMTU[0][mtu] = timestamp;
        if(caughtUp) {
            accumulateForAverages(timestamp,mtu,power);
        }
    }

    public void accumulateForAverages(int timestamp, byte mtu, int power) throws DatabaseException {
        for(int i = 1; i < durations.length; i++) {
            if(timestamp > maxForMTU[i][mtu]) {
                maxForMTU[i][mtu] = timestamp;
                if(timestamp >= start[i][mtu] + durations[i]) {
                    if(count[i][mtu]>0) {
                        int avg = sum[i][mtu]/count[i][mtu];
                        databases[i].put(start[i][mtu], mtu, avg);
                    }
                    sum[i][mtu] = 0;
                    count[i][mtu] = 0;
                    // start at day boundaries, but not dealing with daylight savings time...
                    start[i][mtu] = ((timestamp+timeZoneOffset)/durations[i])*durations[i] - timeZoneOffset;
                }
                sum[i][mtu] += power;
                count[i][mtu]++;
            }
        }
    }


    public void openEnvironment(File envHome) {
        try {
            EnvironmentConfig configuration = new EnvironmentConfig();
            configuration.setLocking(false);
            configuration.setTransactional(false);
//          first one causes out-of-heap problems for me.  The others are probably premature optimization
//            configuration.setCachePercent(90);
//            configuration.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false"); // already off in no-locking mode
//            configuration.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
            configuration.setAllowCreate(true);
            environment = new Environment(envHome, configuration);
            log.info("Environment opened.");
            boolean anyCleaned = false;
            while (environment.cleanLog() > 0) {
                anyCleaned = true;
            }
            if (anyCleaned) {
                log.info("Environment cleaned.");
                CheckpointConfig force = new CheckpointConfig();
                force.setForce(true);
                environment.checkpoint(force);
                log.info("Environment checkpointed.");
            }
            else {
                log.info("No environment clean needed.");
            }
        }
        catch(Throwable e) {
            e.printStackTrace();
            close();
            System.exit(1);
        }
    }

    public void setupMTUsAndArrays(byte mtus) {
        this.mtus = mtus;
        maxForMTU = new int[durations.length][mtus];
        start = new int[durations.length][mtus];
        sum = new int[durations.length][mtus];
        count = new int[durations.length][mtus];
        for(int i = 0; i<durations.length; i++) {
            Arrays.fill(sum[i], 0);
            Arrays.fill(count[i], 0);
        }
    }

    public void openDatabases() throws DatabaseException {
        for(int i = 0; i < durations.length; i++) {
            databases[i] = new TimeSeriesDatabase(environment, String.valueOf(durations[i]), mtus, durations[i], durationStrings[i]);
            log.info("Database " + i + " opened");
            for(byte mtu = 0; mtu < mtus; mtu++) {
                start[i][mtu] = databases[i].maxForMTU(mtu) + durations[i];
                maxForMTU[i][mtu] = start[i][mtu] - 1;
                log.info("   starting at " + start[i][mtu] + " for MTU " + mtu);
            }
        }
        minimum = databases[0].minimum();
        log.info("Minimum is " + minimum);
        int newMax = Integer.MAX_VALUE;
        for(byte mtu = 0; mtu < mtus; mtu++) {
            if(maxForMTU[0][mtu] < newMax) newMax = maxForMTU[0][mtu];
        }
        maximum = newMax;
        log.info("Maximum is " + maximum);
    }

    public void close() {
        for(TimeSeriesDatabase db : databases) {
            if(db!=null) {
                db.close();
            }
        }
        if(environment!=null) {
            try {
                environment.close();
            }
            catch(Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void catchup() {
        int[] caughtUpTo = new int[mtus];
        Arrays.fill(caughtUpTo, Integer.MAX_VALUE);
        for (int i = 1; i < durations.length; i++) {
            for(byte mtu = 0; mtu < mtus; mtu++) {
                if(start[i][mtu] - 1 < caughtUpTo[mtu]) {
                    caughtUpTo[mtu] = start[i][mtu] - 1;
                }
            }
        }
        try {
            // find starting place
            int catchupStart = Integer.MAX_VALUE;
            for(byte mtu = 0; mtu < mtus; mtu++) {
                if(caughtUpTo[mtu] + 1 < catchupStart) {
                    catchupStart = caughtUpTo[mtu] + 1;
                }
            }
            // read the values
            log.info("Catching up from " + catchupStart);
            ReadIterator iter = databases[0].read(catchupStart);
            try {
                while(iter.hasNext()) {
                    Triple triple = iter.next();
                    if(triple.timestamp > caughtUpTo[triple.mtu]){ 
                        accumulateForAverages(triple.timestamp, triple.mtu, triple.power);
                        caughtUpTo[triple.mtu] = triple.timestamp;
                    }
                }
            }
            finally {
                iter.close();
            }
            log.info("Catch-up done.");
        }
        catch(DatabaseException e) {
            e.printStackTrace();
        }
    }

    public void findInitialAdjustment() throws Exception {
        int now = (int)(System.currentTimeMillis()/1000);
        log.trace("Importing 10 seconds for MTU 0 for initial adjustment");
        Iterator<Triple> iter = new ImportIterator(gatewayURL, (byte)0, 10);
        int max = 0;
        while(iter.hasNext()) {
            int ts = iter.next().timestamp;
            if(ts>max) max = ts;
        }
        adjustment = now - max;
        log.trace("Adjustment is: " + adjustment);
    }
    
    public void doImport() throws Exception {
        int newMax = Integer.MAX_VALUE;
        for(byte mtu = 0; mtu < mtus; mtu++) {
//            for(byte mtu = (byte)(mtus-1); mtu >= 0; mtu--) {
            int now = (int)(System.currentTimeMillis()/1000);
            int count = now - maxForMTU[0][mtu] - adjustment;
            if(count <= 0) count = 0;
            count = count + importOverlap;
            if(count > 3600) count = 3600;
            log.trace("Importing " + count + " seconds for MTU " + mtu);
            Iterator<Triple> iter = new ImportIterator(gatewayURL, mtu, count);
            log.trace("Receiving...");
            PriorityQueue<Triple> reversedTriples = new PriorityQueue<Triple>(count,Triple.COMPARATOR);
            for(int i = 0; i < count && iter.hasNext(); i++) {
                Triple next = iter.next();
                if(next==null) break;
//                log.trace("TS: " + next.timestamp + ", max:" + maxForMTU[0][next.mtu]);
                if(next.timestamp > maxForMTU[0][next.mtu]) {
                    reversedTriples.offer(next);
                }
            }
            log.trace("Reversed...");
            Triple triple = reversedTriples.peek();
            if(triple!=null && (minimum==0 || triple.timestamp < minimum)) minimum = triple.timestamp;
            while((triple = reversedTriples.poll()) != null) {
                if(triple.timestamp > maxForMTU[0][triple.mtu]) { 
                    put(triple.timestamp,triple.mtu,triple.power);
                }
            }
            log.trace("Put to " + maxForMTU[0][mtu] + ".");
            adjustment = now - maxForMTU[0][mtu];
            log.trace("Adjustment is: " + adjustment);
            if(maxForMTU[0][mtu] < newMax) newMax = maxForMTU[0][mtu];
        }
        maximum = newMax;

        log.trace("Syncing databases...");
        for(int i = 0; i < durations.length; i++) {
            try {
                databases[i].sync();
            }
            catch(DatabaseException e) {
                e.printStackTrace();
            }
        }
        log.trace("Sync complete.");
        log.trace("Cleaning environment...");
        int cleaned = environment.cleanLog();
        log.trace("Cleaned " + cleaned + " database log files.");
    }

    public void run() {
        try {
            findInitialAdjustment();
            doImport();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        catchup();
        caughtUp = true;
        while(true) {
            try {
                doImport();
                Thread.sleep(importInterval*1000);
            }
            catch(InterruptedException e) {
                close();
                break;
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static final void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("p","port",true,"port served by datasource server (default 8081)");
        options.addOption("m","mtus",true,"number of MTUs (default 1)");
        options.addOption("g","gateway-url",true,"URL of TED 5000 gateway (default http://TED5000)");
        options.addOption("u","username",true,"username for password-protected TED gateway (will prompt for password; default none)");
        options.addOption("n","num-points",true,"target number of data points returned over the zoom region (default 1000)");
        options.addOption("x","max-points",true,"number of data points beyond which server will not go (default 5000)");
        options.addOption("l","server-log",true,"server request log filename; include string \"yyyy_mm_dd\" for automatic rollover; or use \"stderr\" (default no log)");
        options.addOption("i","import-interval",true,"seconds between imports of data (default 15)");
        options.addOption("o","import-overlap",true,"extra seconds imported each time for good measure (default 30)");        
        options.addOption("h","help",false,"print this help text");
        
        // create the parser
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        boolean showUsageAndExit = false;
        try {
            // parse the command line arguments
            cmd = parser.parse(options, args);
        }
        catch(ParseException exp) {
            // oops, something went wrong
            System.err.println(exp.getMessage());
            showUsageAndExit = true;
        }

        final Main main = new Main();
        
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
            System.err.print("Please enter password for username '" + main.username + "': ");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            main.password = reader.readLine();
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
        
        String dbFilename = null;
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
                    options,
                    "\nThe specified database-directory (REQUIRED) is the location of the database.");
        }
        else {
            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(main.username, main.password.toCharArray());
                }
            });
            
            main.setupMTUsAndArrays(main.mtus); 
            File dbFile = new File(dbFilename);
            dbFile.mkdirs();
            main.openEnvironment(dbFile);
            main.openDatabases();
            
            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    main.close();
                }
            });
            
            Servlet.startServlet(main);
            main.run();
        }
    }
}
