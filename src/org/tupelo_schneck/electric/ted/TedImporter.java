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

package org.tupelo_schneck.electric.ted;

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
import org.tupelo_schneck.electric.CatchUp;
import org.tupelo_schneck.electric.DatabaseManager;
import org.tupelo_schneck.electric.Importer;
import org.tupelo_schneck.electric.Main;
import org.tupelo_schneck.electric.Options;
import org.tupelo_schneck.electric.Servlet;
import org.tupelo_schneck.electric.Triple;
import org.tupelo_schneck.electric.Util;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;

public class TedImporter implements Importer {
    private Log log = LogFactory.getLog(TedImporter.class);

    private final Main main;
    private final DatabaseManager databaseManager;
    private final Options options;
    private final Servlet servlet;
    private final CatchUp catchUp;
    
    // volatility is correct; we change the reference on update
    // maxSecondForMTU is used only for ensuring that old-only multi-imports don't consider new data
    private volatile int[] maxSecondForMTU;
    private Object minMaxLock = new Object();
        
    private volatile int latestVoltAmperesTimestamp;

    private ExecutorService longImportTask;
    private ExecutorService shortImportTask;
    private ExecutorService voltAmpereImportTask;
    
    private volatile CountDownLatch importInterleaver;

    public TedImporter(Main main, Options options, DatabaseManager databaseManager, Servlet servlet, CatchUp catchUp) {
        this.main = main;
        this.databaseManager = databaseManager;
        this.options = options;
        this.servlet = servlet;
        this.catchUp = catchUp;
        maxSecondForMTU = databaseManager.secondsDb.maxForMTU.clone();
    }

    @Override
    public void startup() {
        if(options.longImportInterval>0) {
            longImportTask = repeatedlyImport(3600, true, options.longImportInterval);
        }
        if(options.importInterval>0) {
            shortImportTask = repeatedlyImport(options.importInterval + options.importOverlap, false, options.importInterval);
        }
        if(options.voltAmpereImportIntervalMS>0) {
            ExecutorService voltAmpereExecServ;
            if(options.kvaThreads==0) voltAmpereExecServ = shortImportTask;
            else voltAmpereExecServ = Executors.newScheduledThreadPool(options.kvaThreads);
            voltAmpereImportTask = voltAmpereImporter(voltAmpereExecServ, options.voltAmpereImportIntervalMS);
        }
    }
    
    @Override
    public void shutdown() {
        try { longImportTask.shutdownNow(); } catch (Throwable e) {}
        try { shortImportTask.shutdownNow(); } catch (Throwable e) {}
        try { voltAmpereImportTask.shutdownNow(); } catch (Throwable e) {}
        //try { if(importInterleaver!=null) importInterleaver.countDown(); } catch (Throwable e) {}
        try { longImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
        try { shortImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
        try { voltAmpereImportTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
    }
    

    private static class MinAndMax {
        final int min;
        final int max;
        MinAndMax(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }
    
    private MinAndMax changesFromImport(int count, byte mtu, boolean oldOnly) {
        if(!main.isRunning) return null;

        boolean changed = false;
        int minChange = Integer.MAX_VALUE;
        int maxChange = 0;

        log.trace("Importing " + count + " seconds for MTU " + mtu);
        ImportIterator iter = null;
        Cursor cursor = null;
        try {
            iter = new ImportIterator(options, mtu, count);
            cursor = databaseManager.secondsDb.openCursor();
            while(iter.hasNext()) {
                if(!main.isRunning) return null;

                Triple triple = iter.next();
                if(triple==null) break;
                int max = maxSecondForMTU[triple.mtu];
                if(oldOnly && max > 0 && triple.timestamp > max) continue;
                if (databaseManager.secondsDb.putIfChanged(cursor, triple)) {
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

    private class MultiImporter implements Runnable {
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
                main.shutdown();
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
            int[] newMaxForMTU = new int[options.mtus + options.spyders];
            List<Triple.Key> changes = new ArrayList<Triple.Key>();

            for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                if(!main.isRunning) return;
                
                MinAndMax minAndMax = changesFromImport(count,mtu,longImport && options.importInterval > 0);

                if(minAndMax==null) continue;
                
                if(minAndMax.min < newMin) newMin = minAndMax.min;
                newMaxForMTU[mtu] = minAndMax.max;
                changes.add(new Triple.Key(minAndMax.min,mtu));
            }
            int[] maxSeconds = newMaxForMTU.clone();
            Arrays.sort(maxSeconds);
            int latestMax = maxSeconds[options.mtus + options.spyders - 1];
            for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                if(maxSeconds[mtu] + CatchUp.LAG > latestMax) {
                    newMax = maxSeconds[mtu];
                    break;
                }
            }

            if(servlet!=null) {
                servlet.setMinimumIfNewer(newMin);
                servlet.setMaximumIfNewer(newMax);
            }
            int latestVA = latestVoltAmperesTimestamp;
            if(latestVA + CatchUp.LAG < newMax || latestVA > newMax) catchUp.setMaximumIfNewer(newMax);
            else catchUp.setMaximumIfNewer(latestVA);

            catchUp.notifyChanges(changes, false);
            
            synchronized(minMaxLock) {
                for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                    if(newMaxForMTU[mtu] < maxSecondForMTU[mtu]) {
                        newMaxForMTU[mtu] = maxSecondForMTU[mtu];
                    }
                }
                maxSecondForMTU = newMaxForMTU;
            }
            
            if(longImport || options.longImportInterval==0) {
                try {
                    databaseManager.environment.sync();
                    log.trace("Environment synced.");
                    log.trace(databaseManager.environment.getStats(StatsConfig.DEFAULT).toString());
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
    
    private class VoltAmpereImporter implements Runnable {
        public VoltAmpereImporter() {}
        
        @Override
        public void run() {
            try {
                runReal();
            }
            catch(Throwable e) {
                e.printStackTrace();
                main.shutdown();
            }
        }
        
        private void runReal() {
            List<Triple.Key> changes = new ArrayList<Triple.Key>();
            boolean changed = false;

            VoltAmpereFetcher fetcher = new VoltAmpereFetcher(options);
            List<Triple> triples = fetcher.doImport();

            if(triples.isEmpty()) return;
            
            int timestamp = 0;
            Cursor cursor = null;
            try {
                if(!main.isRunning) return;
                cursor = databaseManager.secondsDb.openCursor();
                for(Triple triple : triples) {
                    if(!main.isRunning) return;
                    timestamp = triple.timestamp;
                    if (databaseManager.secondsDb.putIfChanged(cursor, triple)) {
                        changed = true;
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
                catchUp.notifyChanges(changes, true);
            }
        }
    }
    
    private ExecutorService repeatedlyImport(int count,boolean longImport,int interval) {
        ScheduledExecutorService execServ = Executors.newSingleThreadScheduledExecutor();
        execServ.scheduleAtFixedRate(new MultiImporter(count, longImport), 0, interval, TimeUnit.SECONDS);
        return execServ;
    }

    private ExecutorService voltAmpereImporter(final ExecutorService execServ, long interval) {
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

}
