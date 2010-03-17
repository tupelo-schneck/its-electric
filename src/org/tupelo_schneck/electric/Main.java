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

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class Main {
    private Log log = LogFactory.getLog(Main.class);
    
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };
    public static final String[] durationStrings = new String[] {
        "1\"","4\"","15\"","1'","4'","15'","1h","3h","8h","1d"
    };

    public Environment environment;
    public TimeSeriesDatabase[] databases = new TimeSeriesDatabase[durations.length];

    public Options options = new Options();
    
    public int adjustment; // system time minus gateway time
    
    public int minimum;
    public volatile int maximum;
    
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
                    start[i][mtu] = ((timestamp+options.timeZoneRawOffset)/durations[i])*durations[i] - options.timeZoneRawOffset;
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
            // this seems to help with memory issues
            configuration.setCachePercent(40);
            long maxMem = Runtime.getRuntime().maxMemory();
            if(maxMem/100*(100-configuration.getCachePercent()) < 48 * 1024 * 1024) {
                configuration.setCacheSize(maxMem - 48 * 1024 * 1024);
                log.info("Cache size set to: " + (maxMem - 48 * 1024 * 1024));
            }
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
            databases[i] = new TimeSeriesDatabase(environment, String.valueOf(durations[i]), options.mtus, durations[i], durationStrings[i]);
            log.info("Database " + i + " opened");
            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                start[i][mtu] = databases[i].maxForMTU(mtu) + durations[i];
                maxForMTU[i][mtu] = start[i][mtu] - 1;
                log.info("   starting at " + start[i][mtu] + " for MTU " + mtu);
            }
        }
        minimum = databases[0].minimum();
        log.info("Minimum is " + minimum);
        int newMax = Integer.MAX_VALUE;
        for(byte mtu = 0; mtu < options.mtus; mtu++) {
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
        int[] caughtUpTo = new int[options.mtus];
        Arrays.fill(caughtUpTo, Integer.MAX_VALUE);
        for (int i = 1; i < durations.length; i++) {
            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                if(start[i][mtu] - 1 < caughtUpTo[mtu]) {
                    caughtUpTo[mtu] = start[i][mtu] - 1;
                }
            }
        }
        try {
            // find starting place
            int catchupStart = Integer.MAX_VALUE;
            for(byte mtu = 0; mtu < options.mtus; mtu++) {
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
        Iterator<Triple> iter = new ImportIterator(options.gatewayURL, (byte)0, 10);
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
        for(byte mtu = 0; mtu < options.mtus; mtu++) {
            int now = (int)(System.currentTimeMillis()/1000);
            int count = now - maxForMTU[0][mtu] - adjustment;
            if(count <= 0) count = 0;
            count = count + options.importOverlap;
            if(count > 3600) count = 3600;
            log.trace("Importing " + count + " seconds for MTU " + mtu);
            Iterator<Triple> iter = new ImportIterator(options.gatewayURL, mtu, count);
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
//        if (cleaned>0) {
//            CheckpointConfig force = new CheckpointConfig();
//            force.setForce(true);
//            environment.checkpoint(force);
//            log.info("Environment checkpointed.");
//        }
//        System.gc();
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
                Thread.sleep(options.importInterval*1000);
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
        
        if(main.options.parseOptions(args)) {
            main.setupMTUsAndArrays(main.options.mtus); 
            File dbFile = new File(main.options.dbFilename);
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
