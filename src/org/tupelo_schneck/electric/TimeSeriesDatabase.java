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

import java.util.Iterator;

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
    Database database;
    Cursor writeCursor;
    int resolution;
    String resolutionString;

    public static final DatabaseConfig DEFERRED_WRITE_CONFIG;
    static {
        DatabaseConfig config = new DatabaseConfig();
        config.setDeferredWrite(true);
        config.setAllowCreate(true);
        DEFERRED_WRITE_CONFIG = config;
    }

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
        res |= (((int)buf[offset+0] & 0xFF) << 24); 
        res |= (((int)buf[offset+1] & 0xFF) << 16); 
        res |= (((int)buf[offset+2] & 0xFF) << 8); 
        res |= (((int)buf[offset+3] & 0xFF));
        return res;
    }

    public static int intOfVariableBytes(byte[] buf) {
        if(buf.length==0) return 0;
        int res = 0;
        res |= (((int)buf[0] & 0xFF) << 24);
        if(buf.length==1) {
            return res >> 24;
        }
        res |= (((int)buf[1] & 0xFF) << 16); 
        if(buf.length==2) {
            return res >> 16;
        }
        res |= (((int)buf[2] & 0xFF) << 8); 
        if(buf.length==3) {
            return res >> 8;
        }
        res |= (((int)buf[3] & 0xFF));
        return res;

    }
    
    // writing reuses the same key and data---does this actually save memory?
    private byte[] keyBuf = new byte[5];
    private byte[] dataBuf = new byte[4];
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry data = new DatabaseEntry();

    private DatabaseEntry _keyEntry(DatabaseEntry entry, byte[] buf, int timestamp, byte mtu) {
        buf[0] = (byte) ((timestamp >> 24) & 0xFF);
        buf[1] = (byte) ((timestamp >> 16) & 0xFF);
        buf[2] = (byte) ((timestamp >> 8) & 0xFF);
        buf[3] = (byte) (timestamp & 0xFF);
        buf[4] = mtu;
        entry.setData(buf);
        return entry;
    }

    private DatabaseEntry reusedKeyEntry(int timestamp, byte mtu) {
        return _keyEntry(key,keyBuf,timestamp,mtu);
    }

    private DatabaseEntry newKeyEntry(int timestamp, byte mtu) {
        return _keyEntry(new DatabaseEntry(),new byte[5],timestamp,mtu);
    }
    
    private DatabaseEntry _dataEntry(DatabaseEntry entry, byte[] buf, int power) {
        buf[0] = (byte) ((power >> 24) & 0xFF);
        buf[1] = (byte) ((power >> 16) & 0xFF);
        buf[2] = (byte) ((power >> 8) & 0xFF);
        buf[3] = (byte) (power & 0xFF);
        int size;
        if(power==0) size = 0;
        else if(power<=127 && power>=-128) size = 1;
        else if(power<=32767 && power>=-32768) size = 2;
        else if(power<=8388607 && power>=-8388608) size = 3;
        else size = 4;
        entry.setData(buf,4-size,size);
        return entry;
    }

    private DatabaseEntry reusedDataEntry(int power) {
        return _dataEntry(data,dataBuf,power);
    }

    public TimeSeriesDatabase(Environment environment, String name, byte mtus, int resolution, String resolutionString) {
        try {
            this.resolution = resolution;
            this.resolutionString = resolutionString;
            database = environment.openDatabase(null, name, DEFERRED_WRITE_CONFIG);
            writeCursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
        }
        catch(Throwable e) {
            e.printStackTrace();
            close();
            System.exit(1);
        }
    }

    public void sync() throws DatabaseException {
        database.sync();
    }

    public void close() {
        if(writeCursor!=null) {
            try {
                writeCursor.close();
            }
            catch(Throwable e) {
                e.printStackTrace();
            }
        }
        if(database!=null) {
            try {
                database.close();
            }
            catch(Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void put(int timestamp, byte mtu, int power) throws DatabaseException {
        OperationStatus status;
        status = writeCursor.put(reusedKeyEntry(timestamp, mtu), reusedDataEntry(power));
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
    }

    public int maxForMTU(byte mtu) throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
        OperationStatus status = cursor.getLast(key, data, LockMode.READ_UNCOMMITTED);
        try {
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
            try {
                cursor.close();
            }
            catch (Throwable t) {}
        }
    }

    public int minimum() throws DatabaseException {
        ReadIterator iter = new  ReadIterator(0,-1);
        if(!iter.hasNext()) return 0;
        int res = iter.next().timestamp;
        iter.close();
        return res;
    }
    
    public ReadIterator read(int start, int end) throws DatabaseException {
        return new ReadIterator(start,end);
    }

    public ReadIterator read(int start) throws DatabaseException {
        return new ReadIterator(start,-1);
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
                key = newKeyEntry(start,(byte)0);
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
            if(!closed) try { readCursor.close(); } catch (Throwable e) { e.printStackTrace(); }
            closed = true;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext() {
            return status == OperationStatus.SUCCESS;
        }

        public Triple next() {
            Triple res = new Triple();
            byte[] buf = key.getData();
            res.timestamp = intOfBytes(buf,0);
            res.mtu = buf[4];
            buf = data.getData();
            res.power = intOfVariableBytes(buf);
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
}
