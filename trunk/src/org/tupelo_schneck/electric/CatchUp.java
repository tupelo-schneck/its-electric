/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2012 Robert R. Tupelo-Schneck <schneck@gmail.com>
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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;

public class CatchUp implements Runnable {
    Log log = LogFactory.getLog(CatchUp.class);
    
    // "maximum" can be this many seconds behind the newest data, to ensure we have data from each MTU
    // if an MTU is missing longer than this, only then we proceed
    // also used to prevent the catch-up thread from considering timestamps where we haven't yet seen kVA data;
    // if kVA data is missing longer than this, only then we proceed
    public static final int LAG = 5;

    private final Main main;
    private final Options options;
    private final DatabaseManager databaseManager;
    private final int[] caughtUpTo;
    
    private int[] resetTimestamp;
    private volatile boolean reset;
    private Object resetLock = new Object();
    private volatile boolean newData;
    private Object newDataLock = new Object();

    private volatile int maximum;
    private final Object maximumLock = new Object();
    
    public CatchUp(Main main, Options options, DatabaseManager databaseManager) {
        this.main = main;
        this.options = options;
        this.databaseManager = databaseManager;
        caughtUpTo = new int[options.mtus + options.spyders];
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
        boolean firstTime = true;
        while(main.isRunning) {
            // at start or following a reset, figure out where we are caught up to
            Arrays.fill(caughtUpTo, Integer.MAX_VALUE);
            for (int i = 1; i < DatabaseManager.numDurations; i++) {
                for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                    if(databaseManager.databases[i].maxForMTU[mtu] < caughtUpTo[mtu]) {
                        caughtUpTo[mtu] = databaseManager.databases[i].maxForMTU[mtu];
                    }
                }
            }
            // First time each run we do an extra catchup of two hours.
            // This is because we might have crashed right after getting
            // reset data for an hour ago, but before doing the reset.
            if(firstTime) {
                for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                    caughtUpTo[mtu] -= 7200;
                }
                firstTime = false;
            }
            
            
            while(!reset && main.isRunning) {
                catchUpNewData();
            }
            
            if(!main.isRunning) return;
            
            // perform the reset
            synchronized(resetLock) {
                reset = false;
                for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
                    if(resetTimestamp[mtu]!=0) {
                        for (int i = 1; i < DatabaseManager.numDurations; i++) {
                            databaseManager.databases[i].resetForNewData(resetTimestamp[mtu], mtu);
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
        for(byte mtu = 0; mtu < options.mtus + options.spyders; mtu++) {
            if(caughtUpTo[mtu] + 1 < catchupStart) {
                catchupStart = caughtUpTo[mtu] + 1;
            }
        }
        // read the values
        log.trace("Catching up from " + Util.dateString(catchupStart));
        try {
            ReadIterator iter = null;
            Cursor[] cursors = new Cursor[DatabaseManager.numDurations];
            try {
                newData = false;
                iter = databaseManager.secondsDb.read(catchupStart);
                for(int i = 1; i < DatabaseManager.numDurations; i++) {
                    cursors[i] = databaseManager.databases[i].openCursor();
                }
                while(iter.hasNext() && !reset && main.isRunning) {
                    Triple triple = iter.next();
                    if(triple.timestamp > caughtUpTo[triple.mtu]) {
                        if(triple.timestamp > this.maximum) break;
                        synchronized(resetLock) {
                            for(int i = 1; i < DatabaseManager.numDurations; i++) {
                                databaseManager.databases[i].accumulateForAverages(cursors[i],triple);
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
        if(!newData && !reset && main.isRunning) {
            synchronized(newDataLock) { 
                while(!newData && !reset && main.isRunning) {
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

    public int getMaximum() {
        return this.maximum;
    }
    
    public void setMaximumIfNewer(int newMax) {
        synchronized(maximumLock) {
            if(maximum < newMax) maximum = newMax;
        }
    }
    
    public void notifyChanges(List<Triple.Key> changes, boolean existingDataChangesOnly) {
            if(!main.isRunning || changes==null || changes.isEmpty()) return;
            boolean setReset = false;
            boolean setNewData = false;
            synchronized(resetLock) {
                if(resetTimestamp==null) {
                    resetTimestamp = new int[options.mtus + options.spyders];
    //                Arrays.fill(resetTimestamp,0);
                }
    
                for(Triple.Key change : changes) {
                    int timestamp = change.timestamp;
                    byte mtu = change.mtu;
                    
                    boolean needed = false;
                    if(timestamp < resetTimestamp[mtu]) needed = true;
                    else {
                        for(int i = 1; i < DatabaseManager.numDurations; i++) {
                            if(timestamp <= databaseManager.databases[i].maxForMTU[mtu]) {
                                needed = true;
                                break;
                            }
                        }
                    }
    
                    if(needed) {
                        log.trace("Reset needed at " + Util.dateString(timestamp) + " for MTU " + mtu);
                        resetTimestamp[mtu] = timestamp;
                        setReset = true;
                    }
                    else {
                        setNewData = true;
                    }
                }
    
                if(setReset) reset = true;
                else if(existingDataChangesOnly) return; 
                
                if(setNewData) newData = true;
                
                synchronized(newDataLock) {
                    newDataLock.notify();
                }
            }
        }
}