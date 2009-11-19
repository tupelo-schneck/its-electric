package org.tupelo_schneck.electric;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TimeZone;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class Main {
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };

    public Environment environment;
    public TimeSeriesDatabase[] databases = new TimeSeriesDatabase[durations.length];

    public static boolean DEBUG = true;

    public byte mtus;
    public String gatewayURL;
    public int importOverlap = 30;
    public int importInterval = 15; // seconds
    public int syncInterval = 60*60; // seconds
    public int maxDataPoints = 1000;
    public int port = 8081;

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
                        if(DEBUG && durations[i]>=60) {
                            System.out.println("***Avg put: " + i + " " + start[i][mtu] + " " + mtu + " " + avg);
                        }
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
//            configuration.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
//            configuration.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR, "false");
//            configuration.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
            configuration.setAllowCreate(true);
            environment = new Environment(envHome, configuration);
            if(DEBUG) System.out.println("Environment opened.");
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
            databases[i] = new TimeSeriesDatabase(environment, String.valueOf(durations[i]), mtus);
            if(DEBUG) System.out.println("Database " + i + " opened");
            for(byte mtu = 0; mtu < mtus; mtu++) {
                start[i][mtu] = databases[i].maxForMTU(mtu) + durations[i];
                maxForMTU[i][mtu] = start[i][mtu] - 1;
                if(DEBUG) System.out.println("   starting at " + start[i][mtu] + " for MTU " + mtu);
            }
        }
        minimum = databases[0].minimum();
        if(DEBUG) System.out.println("Minimum is " + minimum);
        int newMax = Integer.MAX_VALUE;
        for(byte mtu = 0; mtu < mtus; mtu++) {
            if(maxForMTU[0][mtu] < newMax) newMax = maxForMTU[0][mtu];
        }
        maximum = newMax;
        if(DEBUG) System.out.println("Maximum is " + maximum);
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
            if(DEBUG) System.out.println("Catching up from " + catchupStart);
            Iterator<Triple> iter = databases[0].read(catchupStart);
            while(iter.hasNext()) {
                Triple triple = iter.next();
                if(triple.timestamp > caughtUpTo[triple.mtu]){ 
                    accumulateForAverages(triple.timestamp, triple.mtu, triple.power);
                    caughtUpTo[triple.mtu] = triple.timestamp;
                }
            }
            if(DEBUG) System.out.println("Catch-up done.");
        }
        catch(DatabaseException e) {
            e.printStackTrace();
        }
    }

    public void doImport() throws Exception {
        int newMax = Integer.MAX_VALUE;
        for(byte mtu = 0; mtu < mtus; mtu++) {
//            for(byte mtu = (byte)(mtus-1); mtu >= 0; mtu--) {
            int count = (int)(System.currentTimeMillis()/1000 - maxForMTU[0][mtu] + importOverlap);
            if(count > 3600 || count <= 0) count = 3600;
            if(DEBUG) System.out.println("Importing " + count + " seconds for MTU " + mtu);
            Iterator<Triple> iter = new ImportIterator(gatewayURL, mtu, count);
            if(DEBUG) System.out.println("Receiving...");
            PriorityQueue<Triple> reversedTriples = new PriorityQueue<Triple>(count,Triple.COMPARATOR);
            for(int i = 0; i < count && iter.hasNext(); i++) {
                Triple next = iter.next();
                if(next==null) break;
                if(next.timestamp > maxForMTU[0][next.mtu]) {
                    reversedTriples.offer(next);
                }
            }
            if(DEBUG) System.out.println("Reversed...");
            Triple triple = reversedTriples.peek();
            if(triple!=null && (minimum==0 || triple.timestamp < minimum)) minimum = triple.timestamp;
            while((triple = reversedTriples.poll()) != null) {
                if(triple.timestamp > maxForMTU[0][triple.mtu]) { 
                    put(triple.timestamp,triple.mtu,triple.power);
                }
            }
            if(DEBUG) System.out.println("Put to " + maxForMTU[0][mtu] + ".");
            if(maxForMTU[0][mtu] < newMax) newMax = maxForMTU[0][mtu];
        }
        maximum = newMax;

        if(DEBUG) System.out.println("Syncing databases...");
        for(int i = 0; i < durations.length; i++) {
            try {
                databases[i].sync();
            }
            catch(DatabaseException e) {
                e.printStackTrace();
            }
        }
        if(DEBUG) System.out.println("Sync complete.");
    }

    public void run() {
        try {
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
        final Main main = new Main();
        main.gatewayURL = "http://192.168.1.99:23048";
        main.setupMTUsAndArrays((byte)2);
        main.openEnvironment(new File("/Users/schneck/electricDb"));
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
