/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2011 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.sleepycat.je.StatsConfig;

public class Main {
    Log log = LogFactory.getLog(Main.class);

    // "maximum" can be this many seconds behind the newest data, to ensure we have data from each MTU
    // if an MTU is missing longer than this, only then we proceed
    // also used to prevent the catch-up thread from considering timestamps where we haven't yet seen kVA data;
    // if kVA data is missing longer than this, only then we proceed
    private static final int LAG = 5;
    
    public boolean readOnly = false; // I set this true when running tests from a different main method
    
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };
    public static final int numDurations = durations.length;
    public static final String[] durationStrings = new String[] {
        "1\"","4\"","15\"","1'","4'","15'","1h","3h","8h","1d"
    };

    private Environment environment;
    public final TimeSeriesDatabase[] databases = new TimeSeriesDatabase[numDurations];
    public final TimeSeriesDatabase secondsDb = databases[0];
    
    public final Options options = new Options();
    
    // minimum and maximum are maintained as defaults for the server
    // maximum is also used to keep the catch-up thread from considering timestamps where we've only seen some MTUs
    public volatile int minimum;
    public volatile int maximum;
    // volatility is correct; we change the reference on update
    // maxSecondForMTU is used only for ensuring that old-only multi-imports don't consider new data
    private volatile int[] maxSecondForMTU;
    private Object minMaxLock = new Object();
        
    public static volatile boolean isRunning = true;
        
    private volatile int latestVoltAmperesTimestamp;
    
    public void openEnvironment(File envHome) throws DatabaseException {
        EnvironmentConfig configuration = new EnvironmentConfig();
        configuration.setTransactional(false);
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
            databases[i] = new TimeSeriesDatabase(this, environment, String.valueOf(durations[i]), options.mtus, durations[i], durationStrings[i], options.serveTimeZone.getRawOffset() / 1000);
            log.trace("Database " + i + " opened");
        }
        minimum = secondsDb.minimumAfter(0);
        maxSecondForMTU = secondsDb.maxForMTU.clone();
        log.trace("Minimum is " + Util.dateString(minimum));
        int[] maxSeconds = maxSecondForMTU.clone();
        Arrays.sort(maxSeconds);
        for(byte mtu = 0; mtu < options.mtus; mtu++) {
            if(maxSeconds[mtu] + LAG > maxSeconds[options.mtus-1]) {
                maximum = maxSeconds[mtu];
                break;
            }
        }
        log.trace("Maximum is " + Util.dateString(maximum));
        
        if(options.deleteUntil > maximum) {
            System.err.println("delete-until option would delete everything, ignoring");
        }
        else {
            // Always delete everything before 2009; got some due to bug in its-electric 1.4
            if(options.deleteUntil < 1230000000) options.deleteUntil = 1230000000;
            if(options.deleteUntil >= minimum) {
                minimum = secondsDb.minimumAfter(options.deleteUntil);
                deleteTask = Executors.newSingleThreadExecutor();
                for(final TimeSeriesDatabase db : databases) {
                    deleteTask.execute(db.new DeleteUntil(options.deleteUntil));
                }                    
            }
        }
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

    private static class MinAndMax {
        final int min;
        final int max;
        MinAndMax(int min, int max) {
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
            iter = new ImportIterator(options, mtu, count);
            cursor = secondsDb.openCursor();
            while(iter.hasNext()) {
                if(!isRunning) return null;

                Triple triple = iter.next();
                if(triple==null) break;
                int max = maxSecondForMTU[triple.mtu];
                if(oldOnly && max > 0 && triple.timestamp > max) continue;
                if (secondsDb.putIfChanged(cursor, triple)) {
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
            log.trace("Put from " + Util.dateString(minChange) + " to " + Util.dateString(maxChange) + ".");
            return new MinAndMax(minChange, maxChange);
        }
        else {
            log.trace("No new data.");
            return null;
        }
    }

    private volatile CountDownLatch importInterleaver;

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
            if(longImport && options.importInterval > 0) {
                importInterleaver = new CountDownLatch(1);
                try {
                    importInterleaver.await();
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            
            int newMin = Integer.MAX_VALUE;
            int newMax = 0;
            int[] newMaxForMTU = new int[options.mtus];
            List<Triple.Key> changes = new ArrayList<Triple.Key>();

            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                if(!isRunning) return;
                
                MinAndMax minAndMax = changesFromImport(count,mtu,longImport && options.importInterval > 0);

                if(minAndMax==null) continue;
                
                if(minAndMax.min < newMin) newMin = minAndMax.min;
                newMaxForMTU[mtu] = minAndMax.max;
                changes.add(new Triple.Key(minAndMax.min,mtu));
            }
            int[] maxSeconds = newMaxForMTU.clone();
            Arrays.sort(maxSeconds);
            int latestMax = maxSeconds[options.mtus-1];
            for(byte mtu = 0; mtu < options.mtus; mtu++) {
                if(maxSeconds[mtu] + LAG > latestMax) {
                    newMax = maxSeconds[mtu];
                    break;
                }
            }

            catchUp.reset(changes, false);
            
            synchronized(minMaxLock) {
                if(minimum==0 || minimum > newMin) minimum = newMin;
                if(maximum < newMax) {
                    int latestVA = latestVoltAmperesTimestamp;
                    if(latestVA + LAG < newMax || latestVA > newMax) catchUp.setMaximum(newMax);
                    else if(catchUp.getMaximum() < latestVA) catchUp.setMaximum(latestVA);
                    maximum = newMax;
                }
                for(byte mtu = 0; mtu < options.mtus; mtu++) {
                    if(newMaxForMTU[mtu] < maxSecondForMTU[mtu]) {
                        newMaxForMTU[mtu] = maxSecondForMTU[mtu];
                    }
                }
                maxSecondForMTU = newMaxForMTU;
            }
            
            if(longImport || options.longImportInterval==0) {
                try {
                    environment.sync();
                    log.trace("Environment synced.");
                    log.trace(environment.getStats(StatsConfig.DEFAULT).toString());
                }
                catch(DatabaseException e) {
                    log.debug("Exception syncing environment: " + e);
                }
            }
            else {
                if(importInterleaver!=null) importInterleaver.countDown();
            }
        }
    }
    
    public class VoltAmpereImporter implements Runnable {
        public VoltAmpereImporter() {}
        
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
        
        public void runReal() {
            int[] newMaxForMTU = new int[options.mtus];
            List<Triple.Key> changes = new ArrayList<Triple.Key>();
            boolean changed = false;

            VoltAmpereFetcher fetcher = new VoltAmpereFetcher(options);
            List<Triple> triples = fetcher.doImport();

            if(triples.isEmpty()) return;
            
            int timestamp = 0;
            Cursor cursor = null;
            try {
                if(!isRunning) return;
                cursor = secondsDb.openCursor();
                for(Triple triple : triples) {
                    if(!isRunning) return;
                    timestamp = triple.timestamp;
                    if (secondsDb.putIfChanged(cursor, triple)) {
                        changed = true;
                        newMaxForMTU[triple.mtu] = timestamp;
                        changes.add(new Triple.Key(timestamp,triple.mtu));
                    }
                }
            }
            catch(DatabaseException e) {
                e.printStackTrace();
            }
            finally {
                if(cursor!=null) try { cursor.close(); } catch (Exception e) { e.printStackTrace(); }
            }
            
            if(changed) {
                log.trace("kVA data at " + Util.dateString(timestamp));
                latestVoltAmperesTimestamp = timestamp;
                catchUp.reset(changes, true);
            }
        }
    }
    
    public ExecutorService repeatedlyImport(int count,boolean longImport,int interval) {
        ScheduledExecutorService execServ = Executors.newSingleThreadScheduledExecutor();
        execServ.scheduleAtFixedRate(new MultiImporter(count, longImport), 0, interval, TimeUnit.SECONDS);
        return execServ;
    }

    public ExecutorService voltAmpereImporter(final ExecutorService execServ, long interval) {
        if(!(execServ instanceof ScheduledExecutorService)) throw new AssertionError();
        if(options.kvaThreads <= 1) {
            ((ScheduledExecutorService)execServ).scheduleAtFixedRate(new VoltAmpereImporter(), 0, interval, TimeUnit.MILLISECONDS);
        }
        else {
            for(int i = 0; i < options.kvaThreads; i++) {
                ((ScheduledExecutorService)execServ).scheduleAtFixedRate(new VoltAmpereImporter(), i*interval, interval*options.kvaThreads, TimeUnit.MILLISECONDS);
            }
        }
        return execServ;
    }

    private Server server;
    private ExecutorService longImportTask;
    private ExecutorService shortImportTask;
    private ExecutorService voltAmpereImportTask;
    private ExecutorService catchUpTask;
    private ExecutorService deleteTask;
    
    private CatchUp catchUp;
    
    public void shutdown() {
        if(isRunning) {
            try { log.info("Exiting."); } catch (Throwable e) {}
            isRunning = false;
            try { longImportTask.shutdownNow(); } catch (Throwable e) {}
            try { shortImportTask.shutdownNow(); } catch (Throwable e) {}
            try { voltAmpereImportTask.shutdownNow(); } catch (Throwable e) {}
            try { deleteTask.shutdownNow(); } catch (Throwable e) {}
            try { catchUpTask.shutdownNow(); } catch (Throwable e) {}
            //try { if(importInterleaver!=null) importInterleaver.countDown(); } catch (Throwable e) {}
            try { longImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            try { shortImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            try { voltAmpereImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            try { deleteTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            //try { synchronized(newDataLock) { newDataLock.notifyAll(); } } catch (Throwable e) {}
            try { catchUpTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            try { server.stop(); } catch (Throwable e) {}
            try { close(); } catch (Throwable e) {}
        }
    }
    
    public void export() throws DatabaseException {
        TimeSeriesDatabase database = databases[Main.numDurations - 1];
        for(int i = Main.numDurations - 2; i >= 0; i--) {
            if(database.resolution <= options.resolution) break;
            database = databases[i];
        }
        
        ReadIterator iter = database.read(options.startTime,options.endTime);
        try {
            if(options.exportByMTU) {
                while(iter.hasNext() && isRunning) {
                    Triple triple = iter.next();
                    System.out.print(Util.dateString(triple.timestamp));
                    System.out.print(",");
                    System.out.print(triple.mtu + 1);
                    System.out.print(",");
                    if(triple.power!=null) System.out.print(triple.power);
                    System.out.print(",");
                    if(triple.voltage!=null) System.out.print((double)triple.voltage.intValue()/20);
                    System.out.print(",");
                    if(triple.voltAmperes!=null) System.out.print(triple.voltAmperes);
                    System.out.println();
                }
            }
            else {
                int lastTime = 0;
                int lastMTU = -1;
                StringBuilder row = new StringBuilder();
                while(iter.hasNext() && isRunning) {
                    Triple triple = iter.next();
                    if(triple.timestamp < lastTime) continue;
                    if(triple.timestamp > lastTime) {
                        if(row.length()>0) {
                            for(int i = lastMTU + 1; i < options.mtus; i++) {
                                row.append(",,,");
                            }
                            System.out.println(row);
                            row.delete(0,row.length());
                        }
                        row.append(Util.dateString(triple.timestamp));
                        lastTime = triple.timestamp;
                        lastMTU = -1;
                    }
                    for(int i = lastMTU + 1; i < triple.mtu; i++) {
                        row.append(",,,");
                    }
                    lastMTU = triple.mtu;
                    row.append(",");
                    if(triple.power!=null) row.append(triple.power);
                    row.append(",");
                    if(triple.voltage!=null) row.append((double)triple.voltage.intValue()/20);
                    row.append(",");
                    if(triple.voltAmperes!=null) row.append(triple.voltAmperes);
                }
                if(row.length()>0) {
                    for(int i = lastMTU + 1; i < options.mtus; i++) {
                        row.append(",,,");
                    }
                    System.out.println(row);
                    row.delete(0,row.length());
                }
            }
        }
        finally {
            iter.close();
        }
    }
    
    public static void main(String[] args) {
        final Main main = new Main();

        try {
            if(!main.options.parseOptions(args)) return;
            main.readOnly = !main.options.record;
            File dbFile = new File(main.options.dbFilename);
            dbFile.mkdirs();
            main.openEnvironment(dbFile);
            
            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    main.shutdown();
                }
            });

            main.openDatabases();


            if(main.options.serve) { 
                main.server = Servlet.setupServlet(main);
                main.server.start();
            }
            if(main.options.record && (main.options.longImportInterval>0 || main.options.importInterval>0 || main.options.voltAmpereImportIntervalMS>0)) {
                main.catchUp = new CatchUp(main); 
                main.catchUpTask = Executors.newSingleThreadExecutor();
                main.catchUpTask.execute(main.catchUp);
                if(main.options.longImportInterval>0) {
                    main.longImportTask = main.repeatedlyImport(3600, true, main.options.longImportInterval);
                }
                if(main.options.importInterval>0) {
                    main.shortImportTask = main.repeatedlyImport(main.options.importInterval + main.options.importOverlap, false, main.options.importInterval);
                }
                if(main.options.voltAmpereImportIntervalMS>0) {
                    ExecutorService voltAmpereExecServ;
                    if(main.options.kvaThreads==0) voltAmpereExecServ = main.shortImportTask;
                    else voltAmpereExecServ = Executors.newScheduledThreadPool(main.options.kvaThreads);
                    main.voltAmpereImportTask = main.voltAmpereImporter(voltAmpereExecServ, main.options.voltAmpereImportIntervalMS);
                }
            }
            if(main.options.export) {
                main.export();
            }
        }
        catch(Throwable e) {
            e.printStackTrace();
            main.shutdown();
        }
    }
}
