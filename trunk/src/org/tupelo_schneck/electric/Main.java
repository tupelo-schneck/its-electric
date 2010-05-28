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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class Main {
    Log log = LogFactory.getLog(Main.class);
    
    boolean readOnly = false; // I set this true when running tests from a different main method
    
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };
    public static final int numDurations = durations.length;
    public static final String[] durationStrings = new String[] {
        "1\"","4\"","15\"","1'","4'","15'","1h","3h","8h","1d"
    };

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
    public static String dateString(int seconds) {
        synchronized(dateFormat) {
            return dateFormat.format(Long.valueOf(1000L * seconds));
        }
    }
    
    private Environment environment;
    public TimeSeriesDatabase[] databases = new TimeSeriesDatabase[numDurations];
    public TimeSeriesDatabase secondsDb;
    
    public Options options = new Options();
    
    public volatile int minimum;
    public volatile int maximum;
    // volatility is correct; we change the reference on update
    private volatile int[] maxSecondForMTU;
    private Object minMaxLock = new Object();
        
    private volatile boolean isRunning = true;
        
    private int[] resetTimestamp;
    private volatile boolean reset;
    private Object resetLock = new Object();
    
    private volatile boolean newData;
    private Object newDataLock = new Object();
    
    public void openEnvironment(File envHome) throws DatabaseException {
        EnvironmentConfig configuration = new EnvironmentConfig();
        configuration.setTransactional(true);
        // this seems to help with memory issues
        configuration.setCachePercent(40);
        long maxMem = Runtime.getRuntime().maxMemory();
        if(maxMem/100*(100-configuration.getCachePercent()) < 60L * 1024 * 1024) {
            configuration.setCacheSize(maxMem - 60L * 1024 * 1024);
            log.info("Cache size set to: " + (maxMem - 60L * 1024 * 1024));
        }
        configuration.setAllowCreate(true);
        configuration.setReadOnly(readOnly);
        environment = new Environment(envHome, configuration);
        log.info("Environment opened.");
    }

    public void openDatabases() throws DatabaseException {
        for(int i = 0; i < numDurations; i++) {
            databases[i] = new TimeSeriesDatabase(this, environment, String.valueOf(durations[i]), options.mtus, durations[i], durationStrings[i]);
            log.trace("Database " + i + " opened");
        }
        secondsDb = databases[0];
        minimum = secondsDb.minimum();
        maxSecondForMTU = secondsDb.maxForMTU.clone();
        log.trace("Minimum is " + dateString(minimum));
        int newMax = 0;
        for(byte mtu = 0; mtu < options.mtus; mtu++) {
            if(maxSecondForMTU[mtu] > newMax) newMax = maxSecondForMTU[mtu];
        }
        maximum = newMax;
        log.trace("Maximum is " + dateString(maximum));
    }

    private boolean closed;
    
    public synchronized void close() {
        if(closed) return;
        for(TimeSeriesDatabase db : databases) {
            if(db!=null) db.close();
        }
        if(environment!=null) try { 
            environment.close(); 
            log.info("Environment closed.");
        } catch (Exception e) { e.printStackTrace(); }
        closed = true;
    }

    private void reset(List<Triple> changes) {
        if(!isRunning || changes==null || changes.isEmpty()) return;
        boolean setReset = false;
        boolean setNewData = false;
        synchronized(resetLock) {
            if(resetTimestamp==null) {
                resetTimestamp = new int[options.mtus];
                Arrays.fill(resetTimestamp,0);
            }

            for(Triple change : changes) {
                int timestamp = change.timestamp;
                byte mtu = change.mtu;
                
                boolean needed = false;
                if(timestamp < resetTimestamp[mtu]) needed = true;
                else {
                    for(int i = 1; i < numDurations; i++) {
                        if(timestamp <= databases[i].maxForMTU[mtu]) {
                            needed = true;
                            break;
                        }
                    }
                }

                if(needed) {
                    log.trace("Reset needed at " + dateString(timestamp) + " for MTU " + mtu);
                    resetTimestamp[mtu] = timestamp;
                    setReset = true;
                }
                else {
                    setNewData = true;
                }
            }

            if(setReset) reset = true;
            if(setNewData) newData = true;
            
            synchronized(newDataLock) {
                newDataLock.notify();
            }
        }
    }

    public class MinAndMax {
        public final int min;
        public final int max;

        public MinAndMax(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }
    
    public MinAndMax changesFromImport(int count, byte mtu, boolean oldOnly) {
        if(!isRunning) return null;

        boolean changed = false;
        int minChange = Integer.MAX_VALUE;
        int maxChange = 0;

        log.trace("Importing " + count + " seconds for MTU " + mtu);
        ImportIterator iter = null;
        Cursor cursor = null;
        try {
            iter = new ImportIterator(options.gatewayURL, mtu, count);
            cursor = secondsDb.openCursor();
            while(iter.hasNext()) {
                if(!isRunning) return null;

                Triple triple = iter.next();
                if(triple==null) break;
                int max = maxSecondForMTU[triple.mtu];
                if(oldOnly && max > 0 && triple.timestamp > max) continue;
                if (secondsDb.putIfChanged(cursor, triple.timestamp, triple.mtu, triple.power)) {
                    changed = true;
                    if(triple.timestamp < minChange) minChange = triple.timestamp;
                    if(triple.timestamp > maxChange) maxChange = triple.timestamp;
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if(iter!=null) try { iter.close(); } catch (Exception e) { e.printStackTrace(); }
            if(cursor!=null) try { cursor.close(); } catch (Exception e) { e.printStackTrace(); }
        }

        if(changed) {
            log.trace("Put from " + dateString(minChange) + " to " + dateString(maxChange) + ".");
            return new MinAndMax(minChange, maxChange);
        }
        else {
            log.trace("No new data.");
            return null;
        }
    }

    public class MultiImporter implements Runnable {
        private int count;
        private boolean longImport;
        
        public MultiImporter(int count, boolean longImport) {
            this.count = count;
            this.longImport = longImport;
        }

        @Override
        public void run() {
            try {
                runReal();
            }
            catch(Throwable e) {
                e.printStackTrace();
                shutdown();
            }
        }
        
        private void runReal() {
            int newMin = Integer.MAX_VALUE;
            int newMax = 0;
            int[] newMaxForMTU = new int[options.mtus];
            List<Triple> changes = new LinkedList<Triple>();

            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                if(!isRunning) return;
                
                MinAndMax minAndMax = changesFromImport(count,mtu,longImport);

                if(minAndMax==null) continue;
                
                if(minAndMax.min < newMin) newMin = minAndMax.min;
                if(newMax < minAndMax.max) newMax = minAndMax.max;
                newMaxForMTU[mtu] = minAndMax.max;
                changes.add(new Triple(minAndMax.min,mtu,0));
            }

            reset(changes);
            
            synchronized(minMaxLock) {
                if(minimum==0 || minimum > newMin) minimum = newMin;
                if(maximum < newMax) maximum = newMax;
                for(byte mtu = 0; mtu < options.mtus; mtu++) {
                    if(newMaxForMTU[mtu] < maxSecondForMTU[mtu]) {
                        newMaxForMTU[mtu] = maxSecondForMTU[mtu];
                    }
                }
                maxSecondForMTU = newMaxForMTU;
            }
            
            try {
                if(longImport) {
                    environment.sync();
                    log.trace("Environment synced.");
                }
            }
            catch(DatabaseException e) {
                log.debug("Exception syncing environment: " + e);
            }
        }
    }
    
    private static class Task {
        private final ExecutorService execServ;
        private final Future<?> future;
        
        public Task(ExecutorService execServ, Future<?> future) {
            this.execServ = execServ;
            this.future = future;
        }
        
        public void stop() {
            try { this.future.cancel(true); } catch (Exception e) {}
            try { this.future.get(); } catch (Exception e) {}
            try { this.execServ.shutdown(); } catch (Exception e) {}
        }
    }
    
    public Task repeatedlyImport(int count,boolean longImport,int interval) {
        ScheduledExecutorService execServ = Executors.newSingleThreadScheduledExecutor();
        Future<?> future = execServ.scheduleAtFixedRate(new MultiImporter(count, longImport), 0, interval, TimeUnit.SECONDS);
        return new Task(execServ,future);
    }

    public class CatchUp implements Runnable {
        private int[] caughtUpTo = new int[options.mtus];

        @Override
        public void run() {
            try {
                runReal();
            }
            catch(Throwable e) {
                e.printStackTrace();
                shutdown();
            }
        }
        
        private void runReal() {
            boolean firstTime = true;
            while(isRunning) {
                // at start or following a reset, figure out where we are caught up to
                Arrays.fill(caughtUpTo, Integer.MAX_VALUE);
                for (int i = 1; i < numDurations; i++) {
                    for(byte mtu = 0; mtu < options.mtus; mtu++) {
                        if(databases[i].maxForMTU[mtu] < caughtUpTo[mtu]) {
                            caughtUpTo[mtu] = databases[i].maxForMTU[mtu];
                        }
                    }
                }
                // First time each run we do an extra catchup of two hours.
                // This is because we might have crashed right after getting
                // reset data for an hour ago, but before doing the reset.
                if(firstTime) {
                    for(byte mtu = 0; mtu < options.mtus; mtu++) {
                        caughtUpTo[mtu] -= 7200;
                    }
                    firstTime = false;
                }
                
                
                while(!reset && isRunning) {
                    catchUpNewData();
                }
                
                if(!isRunning) return;
                
                // perform the reset
                synchronized(resetLock) {
                    reset = false;
                    for(byte mtu = 0; mtu < options.mtus; mtu++) {
                        if(resetTimestamp[mtu]!=0) {
                            for (int i = 1; i < numDurations; i++) {
                                databases[i].resetForNewData(resetTimestamp[mtu], mtu);
                            }
                            resetTimestamp[mtu] = 0;
                        }
                    }
                }
            }
        }
        
        private void catchUpNewData() {
            // find starting place
            int catchupStart = Integer.MAX_VALUE;
            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                if(caughtUpTo[mtu] + 1 < catchupStart) {
                    catchupStart = caughtUpTo[mtu] + 1;
                }
            }
            // read the values
            log.trace("Catching up from " + dateString(catchupStart));
            try {
                ReadIterator iter = null;
                Cursor[] cursors = new Cursor[numDurations];
                try {
                    newData = false;
                    iter = secondsDb.read(catchupStart);
                    for(int i = 1; i < numDurations; i++) {
                        cursors[i] = databases[i].openCursor();
                    }
                    while(iter.hasNext() && !reset && isRunning) {
                        Triple triple = iter.next();
                        if(triple.timestamp > caughtUpTo[triple.mtu]){ 
                            synchronized(resetLock) {
                                for(int i = 1; i < numDurations; i++) {
                                    databases[i].accumulateForAverages(cursors[i],triple.timestamp,triple.mtu,triple.power);
                                }
                            }
                            caughtUpTo[triple.mtu] = triple.timestamp;
                        }
                    }
                }
                finally {
                    for(Cursor cursor : cursors) { if(cursor!=null) try { cursor.close(); } catch (Exception e) { e.printStackTrace(); } }
                    if(iter!=null) try { iter.close(); } catch (Exception e) { e.printStackTrace(); }
                }
                log.trace("Catch-up done.");
            }
            catch(DatabaseException e) {
                e.printStackTrace();
            }
            
            // wait for new data (or a reset)
            if(!newData && !reset && isRunning) {
                synchronized(newDataLock) { 
                    while(!newData && !reset && isRunning) {
                        try {
                            newDataLock.wait();
                        }
                        catch(InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }
    }
    
    private Server server;
    private Task longImportTask;
    private Task shortImportTask;
    private Task catchUpTask;    
    
    public void shutdown() {
        if(isRunning) {
            try { log.info("Exiting."); } catch (Throwable e) {}
            isRunning = false;
            try { longImportTask.stop(); } catch (Throwable e) {}
            try { shortImportTask.stop(); } catch (Throwable e) {}
            try { catchUpTask.stop(); } catch (Throwable e) {}
            try { server.stop(); } catch (Throwable e) {}
            try { close(); } catch (Throwable e) {}
        }
    }
    

    public static final void main(String[] args) {
        final Main main = new Main();

        try {
            if(!main.options.parseOptions(args)) return;
            File dbFile = new File(main.options.dbFilename);
            dbFile.mkdirs();
            main.openEnvironment(dbFile);
            main.openDatabases();

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    main.shutdown();
                }
            });

            main.server = Servlet.startServlet(main);
            main.longImportTask = main.repeatedlyImport(3600, true, main.options.longImportInterval);
            main.shortImportTask = main.repeatedlyImport(main.options.importInterval + main.options.importOverlap, false, main.options.importInterval);
            ExecutorService execServ = Executors.newSingleThreadExecutor();
            main.catchUpTask = new Task(execServ,execServ.submit(main.new CatchUp()));
        }
        catch(Throwable e) {
            e.printStackTrace();
            main.shutdown();
        }
    }
}
