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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class TimeSeriesDatabase {
    Log log = LogFactory.getLog(TimeSeriesDatabase.class);

    private Main main;
    private Database database;

    public int resolution;
    public String resolutionString;

    // start[mtu] is the next entry that the database will get;
    // sum[mtu] and count[mtu] are accumulated to find the average.
    // maxForMTU[mtu] is max processed timestamp for MTU.  Always between maxStoredTimestamp and start
    // These are not relevant for resolution=1 (except secondsDb.maxForMTU used in Main.openDatabases)
    public int[] start;
    public int[] sum;
    public int[] count;
    public int[] maxForMTU;

    public static DatabaseConfig ALLOW_CREATE_CONFIG = null;

    //	public static long longOfBytes(byte[] buf, int offset) {
    //		long res = 0;
    //		res |= (((long)buf[offset+0] & 0xFF) << 56); 
    //		res |= (((long)buf[offset+1] & 0xFF) << 48); 
    //		res |= (((long)buf[offset+2] & 0xFF) << 40); 
    //		res |= (((long)buf[offset+3] & 0xFF) << 32);		
    //		res |= (((long)buf[offset+4] & 0xFF) << 24); 
    //		res |= (((long)buf[offset+5] & 0xFF) << 16); 
    //		res |= (((long)buf[offset+6] & 0xFF) << 8); 
    //		res |= (((long)buf[offset+7] & 0xFF));
    //		return res;
    //	}

    public static int intOfBytes(byte[] buf, int offset) {
        int res = 0;	
        res |= ((buf[offset+0] & 0xFF) << 24); 
        res |= ((buf[offset+1] & 0xFF) << 16); 
        res |= ((buf[offset+2] & 0xFF) << 8); 
        res |= ((buf[offset+3] & 0xFF));
        return res;
    }

    public static int intOfVariableBytes(byte[] buf) {
        if(buf.length==0) return 0;
        int res = 0;
        res |= ((buf[0] & 0xFF) << 24);
        if(buf.length==1) {
            return res >> 24;
        }
        res |= ((buf[1] & 0xFF) << 16); 
        if(buf.length==2) {
            return res >> 16;
        }
        res |= ((buf[2] & 0xFF) << 8); 
        if(buf.length==3) {
            return res >> 8;
        }
        res |= ((buf[3] & 0xFF));
        return res;

    }
    
    private DatabaseEntry keyEntry(int timestamp, byte mtu) {
        byte[] buf = new byte[5];
        buf[0] = (byte) ((timestamp >> 24) & 0xFF);
        buf[1] = (byte) ((timestamp >> 16) & 0xFF);
        buf[2] = (byte) ((timestamp >> 8) & 0xFF);
        buf[3] = (byte) (timestamp & 0xFF);
        buf[4] = mtu;
        return new DatabaseEntry(buf);
    }
    
    private DatabaseEntry dataEntry(int power) {
        int size;
        if(power==0) size = 0;
        else if(power<=127 && power>=-128) size = 1;
        else if(power<=32767 && power>=-32768) size = 2;
        else if(power<=8388607 && power>=-8388608) size = 3;
        else size = 4;
        byte[] buf = new byte[4];
        buf[0] = (byte) ((power >> 24) & 0xFF);
        buf[1] = (byte) ((power >> 16) & 0xFF);
        buf[2] = (byte) ((power >> 8) & 0xFF);
        buf[3] = (byte) (power & 0xFF);
        return new DatabaseEntry(buf,4-size,size);
    }

    public TimeSeriesDatabase(Main main, Environment environment, String name, byte mtus, int resolution, String resolutionString) {
        try {
            this.main = main;
            this.resolution = resolution;
            this.resolutionString = resolutionString;
            synchronized(TimeSeriesDatabase.class) {
                if(ALLOW_CREATE_CONFIG==null) {
                    DatabaseConfig config = new DatabaseConfig();
                    config.setAllowCreate(true);
                    config.setReadOnly(main.readOnly);
                    ALLOW_CREATE_CONFIG = config;
                }
            }
            database = environment.openDatabase(null, name, ALLOW_CREATE_CONFIG);
            
            maxForMTU = new int[mtus];
            start = new int[mtus];
            sum = new int[mtus];
            count = new int[mtus];

            Arrays.fill(sum, 0);
            Arrays.fill(count, 0);
            
            for(byte mtu = 0; mtu < mtus; mtu++) {
                start[mtu] = maxStoredTimestampForMTU(mtu) + resolution;
                maxForMTU[mtu] = start[mtu] - 1;
                log.trace("   starting at " + Main.dateString(start[mtu]) + " for MTU " + mtu);
            }

            // Delete everything before 2009; got some due to bug in its-electric 1.4
            Cursor cursor = openCursor();
            try {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry readDataEntry = new DatabaseEntry();
                while(true) {
                    key = keyEntry(0,(byte)0);
                    OperationStatus status = cursor.getSearchKeyRange(key, readDataEntry, LockMode.READ_UNCOMMITTED);
                    if(status!=OperationStatus.SUCCESS) break;
                    byte[] buf = key.getData();
                    int timestamp = intOfBytes(buf,0);
                    if(timestamp<1230000000) {
                        log.info("Deleting " + Main.dateString(timestamp));
                        database.delete(null,key);
                    }
                    else break;
                }
            }
            finally {
                cursor.close();
            }
        }
        catch(Throwable e) {
            e.printStackTrace();
            close();
            main.shutdown();
        }
    }

    public void close() {
        if(database!=null) try { database.close(); } catch(Exception e) { e.printStackTrace(); }
    }

    public void put(Cursor cursor,int timestamp, byte mtu, int power) throws DatabaseException {
        OperationStatus status;
        status = cursor.put(keyEntry(timestamp, mtu), dataEntry(power));
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
    }
    
    public Cursor openCursor() throws DatabaseException {
        return database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
    }
    
    public boolean putIfChanged(Cursor cursor, int timestamp, byte mtu, int power) throws DatabaseException {
        OperationStatus status;
        DatabaseEntry key = keyEntry(timestamp,mtu);
        DatabaseEntry data = dataEntry(power);
        status = cursor.putNoOverwrite(key, data);
        if(status==OperationStatus.SUCCESS) return true;
        if(status!=OperationStatus.KEYEXIST) {
            throw new DatabaseException("Unexpected status " + status);
        }

        DatabaseEntry readDataEntry = new DatabaseEntry();
        status = cursor.getSearchKey(key,readDataEntry,LockMode.READ_UNCOMMITTED);
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
        if(power==intOfVariableBytes(readDataEntry.getData())) return false;

        status = cursor.put(key, data);
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
        return true;
    }

    private int maxStoredTimestampForMTU(byte mtu) throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = null;
        try {
            cursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
            OperationStatus status = cursor.getLast(key, data, LockMode.READ_UNCOMMITTED);
            while(status == OperationStatus.SUCCESS) {
                byte[] buf = key.getData();
                if(buf[4]==mtu) {
                    return intOfBytes(buf,0);
                }
                status = cursor.getPrev(key, data, LockMode.READ_UNCOMMITTED);
            }
            return 0;
        }
        finally {
            if(cursor!=null) try { cursor.close(); } catch (Throwable t) {}
        }
    }

    public int minimum() throws DatabaseException {
        ReadIterator iter = read(0);
        if(!iter.hasNext()) return 0;
        int res = iter.next().timestamp;
        iter.close();
        return res;
    }
    
    public ReadIterator read(int startDate, int endDate) throws DatabaseException {
        return new ReadIterator(startDate,endDate);
    }

    public ReadIterator read(int startDate) throws DatabaseException {
        return new ReadIterator(startDate,-1);
    }

    public class ReadIterator implements Iterator<Triple> {
        Cursor readCursor;
        DatabaseEntry key;
        DatabaseEntry data;
        OperationStatus status;
        int end;
        boolean closed;

        public ReadIterator (int start, int end) throws DatabaseException {
            if(end<0 || end>=start) {
                this.end = end;
                readCursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
                key = keyEntry(start,(byte)0);
                data = new DatabaseEntry();
                status = readCursor.getSearchKeyRange(key, data, LockMode.READ_UNCOMMITTED);
                closeIfNeeded();
            }
            else { closed = true; }
        }

        private void closeIfNeeded() {
            if(status==OperationStatus.SUCCESS) {
                if(end>=0) {
                    byte[] buf = key.getData();
                    if(intOfBytes(buf,0) > end) {
                        status = null;
                        close();
                    }
                }
            }
            else {
                close();
            }
        }
        
        public void close() {
            if(!closed) try { readCursor.close(); } catch (Exception e) { e.printStackTrace(); }
            closed = true;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext() {
            return status == OperationStatus.SUCCESS;
        }

        public Triple next() {
            byte[] buf = key.getData();
            int timestamp = intOfBytes(buf,0);
            byte mtu = buf[4];
            buf = data.getData();
            int power = intOfVariableBytes(buf);
            Triple res = new Triple(timestamp,mtu,power);
            try {
                status = readCursor.getNext(key, data, LockMode.READ_UNCOMMITTED);
                closeIfNeeded();
            }
            catch(DatabaseException e) {
                e.printStackTrace();
                status = null;
            }
            return res;
        }
    }
    
    // not relevant for resolution=1
    public void accumulateForAverages(Cursor cursor,int timestamp, byte mtu, int power) throws DatabaseException {
        if(timestamp > maxForMTU[mtu]) {
            maxForMTU[mtu] = timestamp;
            if(timestamp >= start[mtu] + resolution) {
                if(count[mtu]>0) {
                    int avg = sum[mtu]/count[mtu];
                    put(cursor,start[mtu], mtu, avg);
                }
                sum[mtu] = 0;
                count[mtu] = 0;
                // start at day boundaries, but not dealing with daylight savings time...
                start[mtu] = ((timestamp+main.options.timeZoneRawOffset)/resolution)*resolution - main.options.timeZoneRawOffset;
            }
            sum[mtu] += power;
            count[mtu]++;
        }
    }
    
    
    // not relevant for resolution=1
    public void resetForNewData(int timestamp, byte mtu) {
        if(maxForMTU[mtu] >= timestamp) {
            start[mtu] = ((timestamp+main.options.timeZoneRawOffset)/resolution)*resolution - main.options.timeZoneRawOffset;
            maxForMTU[mtu] = start[mtu] - 1;
            sum[mtu] = 0;
            count[mtu] = 0;
        }
    }
}
