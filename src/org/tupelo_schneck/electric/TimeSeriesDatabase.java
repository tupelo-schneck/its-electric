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

    byte[] keyBuf = new byte[5];
    byte[] dataBuf = new byte[4];
    DatabaseEntry key = new DatabaseEntry();
    DatabaseEntry data = new DatabaseEntry();

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
    
    public DatabaseEntry keyEntry(DatabaseEntry entry, byte[] buf, int timestamp, byte mtu) {
        buf[0] = (byte) ((timestamp >> 24) & 0xFF);
        buf[1] = (byte) ((timestamp >> 16) & 0xFF);
        buf[2] = (byte) ((timestamp >> 8) & 0xFF);
        buf[3] = (byte) (timestamp & 0xFF);
        buf[4] = mtu;		
        entry.setData(buf);
        return entry;
    }

    public DatabaseEntry keyEntry(int timestamp, byte mtu) {
        return keyEntry(key,keyBuf,timestamp,mtu);
    }

    public DatabaseEntry dataEntry(DatabaseEntry entry, byte[] buf, int power) {
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

    public DatabaseEntry dataEntry(int power) {
        return dataEntry(data,dataBuf,power);
    }

    public TimeSeriesDatabase(Environment environment, String name, byte mtus) {
        try {
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
        status = writeCursor.put(keyEntry(timestamp, mtu), dataEntry(power));
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
        if(status!=OperationStatus.SUCCESS) {
            throw new DatabaseException("Unexpected status " + status);
        }
    }

    public int maxForMTU(byte mtu) throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
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

    public int minimum() throws DatabaseException {
        ReadIterator iter = new  ReadIterator(0,-1);
        if(!iter.hasNext()) return 0;
        int res = iter.next().timestamp;
        iter.close();
        return res;
    }
    
    public Iterator<Triple> read(int start, int end) throws DatabaseException {
        return new ReadIterator(start,end);
    }

    public Iterator<Triple> read(int start) throws DatabaseException {
        return new ReadIterator(start,-1);
    }

    public class ReadIterator implements Iterator<Triple> {
        Cursor readCursor;
        DatabaseEntry key;
        DatabaseEntry data;
        OperationStatus status;
        int end;

        public ReadIterator (int start, int end) throws DatabaseException {
            if(end<0 || end>=start) {
                this.end = end;
                readCursor = database.openCursor(null, CursorConfig.READ_UNCOMMITTED);
                key = keyEntry(new DatabaseEntry(),new byte[5],start,(byte)0);
                data = new DatabaseEntry();
                status = readCursor.getSearchKeyRange(key, data, LockMode.READ_UNCOMMITTED);
                if(Main.DEBUG) System.out.println("Opening cursor from " + start + "; status: " + status);
                closeIfNeeded();
            }
        }

        public void closeIfNeeded() {
            if(status==OperationStatus.SUCCESS) {
                if(end>=0) {
                    byte[] buf = key.getData();
                    if(intOfBytes(buf,0) > end) {
                        status = null;
                        if(Main.DEBUG) System.out.println("Closing for reaching end"); 
                        close();
                    }
                }
            }
            else {
                if(Main.DEBUG) System.out.println("Closing for status: " + status); 
                close();
            }
        }
        
        public void close() {
            try { readCursor.close(); } catch (Throwable e) { e.printStackTrace(); }
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
