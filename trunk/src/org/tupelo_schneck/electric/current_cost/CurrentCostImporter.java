package org.tupelo_schneck.electric.current_cost;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gnu.io.CommPort;
import gnu.io.CommPortIdentifier;
import gnu.io.SerialPort;
import gnu.io.SerialPortEvent;
import gnu.io.SerialPortEventListener;

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

public class CurrentCostImporter implements Importer, SerialPortEventListener {
    static {
        System.out.println("RXTX version mismatch warning can safely be ignored.");
    }

    private Log log = LogFactory.getLog(CurrentCostImporter.class);

    /** Milliseconds to block while waiting for port open */
    private static final int TIME_OUT = 5000;
    private static final int[] SERIAL_SPEEDS = new int[] { 57600, 9600, 2400 }; 
    private static final int MAX_FAILS = 5;
    private static final long ENVIRONMENT_SYNC_INTERVAL = 60*1000;
    private static final long STATS_DUMP_INTERVAL = 5*60*1000;

    private final Main main;
    private final DatabaseManager databaseManager;
    private final Options options;
    private final Servlet servlet;
    private final CatchUp catchUp;

    private String portName;
    private int maxSensor;
    private int numberOfClamps;
    private boolean sumClamps;
    private boolean recordTemperature;

    private SerialPort serialPort;
    private boolean modelVerified;
    private int serialSpeed;
    private InputStream input;
    private StringBuilder sb;
    private int fails;

    private long latestStatsDump;
    private long latestSync;
    
    public CurrentCostImporter(Main main, Options options, DatabaseManager databaseManager, Servlet servlet, CatchUp catchUp) {
        this.main = main;
        this.databaseManager = databaseManager;
        this.options = options;
        this.servlet = servlet;
        this.catchUp = catchUp;

        portName = this.options.ccPortName;
        maxSensor = this.options.ccMaxSensor;
        sumClamps = this.options.ccSumClamps;
        numberOfClamps = this.options.ccNumberOfClamps;
        recordTemperature = false;
    }

    @Override
    public synchronized void startup() {
        initSerial();
    }

    private synchronized void initSerial() {
        try {
            CommPortIdentifier portId = CommPortIdentifier.getPortIdentifier(portName);

            if(portId.isCurrentlyOwned()) {
                System.err.println("Port " + portName + " is currently owned.");
                main.shutdown();
                return;
            }

            CommPort commPort = portId.open(this.getClass().getName(),TIME_OUT);

            if(!(commPort instanceof SerialPort)) {
                System.err.println("Port " + portName + " is not a serial port.");
                main.shutdown();
                return;
            }
            serialPort = (SerialPort) commPort;
            serialPort.setSerialPortParams(SERIAL_SPEEDS[serialSpeed],SerialPort.DATABITS_8,SerialPort.STOPBITS_1,SerialPort.PARITY_NONE);
            input = serialPort.getInputStream();
            sb = new StringBuilder();
            serialPort.addEventListener(this);
            serialPort.notifyOnDataAvailable(true);
        }
        catch(Exception e) {
            e.printStackTrace();
            main.shutdown();
            return;
        }
    }

    private synchronized void closeSerial() {
        if (serialPort != null) {
            serialPort.removeEventListener();
            serialPort.close();
        }
    }

    @Override
    public synchronized void shutdown() {
        closeSerial();
    }

    @Override
    public void serialEvent(SerialPortEvent ev) {
        if(!main.isRunning || ev.getEventType()!=SerialPortEvent.DATA_AVAILABLE) return;

        if(fails > MAX_FAILS) {
            log.error("Too many errors reading from serial port.");
            main.shutdown();
            return;
        }
        
        byte[] buf;
        int r = 0;
        try {
            int available = input.available();
            if(available==0) return;
            buf = new byte[available];
            int off = 0;

            while(main.isRunning && available > 0 && (r = input.read(buf,off,available)) > 0) {
                off += r;
                available -= r;
            }
        }
        catch(IOException e) {
            fails++;
            sb.delete(0,sb.length());
            log.error("Error reading from serial port", e);
            return;
        }

        if(!main.isRunning) return;

        if(r<0) {
            fails++;
            log.error("Error reading from serial port; no input");
            closeSerial();
            try { Thread.sleep(1000); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
            initSerial();
            return;
        }

        fails = 0;

        String s;
        try {
            s = new String(buf,"UTF-8");
        }
        catch(UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
        
        if (!modelVerified) {    
            boolean nonsenseXML = false;
            for(int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                if((ch<32 && ch!=9 && ch!=10 && ch!=13) || (ch>=127 && ch<=159) || ch>255) {
                    nonsenseXML = true;
                    break;
                } 
            }

            if (nonsenseXML) {
                serialSpeed++;
                if(serialSpeed>=SERIAL_SPEEDS.length) { 
                    log.error("Unable to find correct speed for serial port.");
                    main.shutdown();
                    return;
                }
                closeSerial();
                initSerial();
                return;
            }
        } 

        sb.append(s);
        int startPos = sb.indexOf("<msg>");
        int endPos = sb.indexOf("</msg>");

        if ((startPos >=0) && (endPos >= 0)){
            if (endPos > startPos){     
                modelVerified = true;
                String message = sb.substring(startPos, endPos+6);
                sb.delete(0,endPos+6);
                List<Triple> triples = currentCostParse(message);
                processTriples(triples);
            } 
            else {                
                sb.delete(0,startPos);
            }
        }

    }

    private static final Pattern sensorPattern = Pattern.compile("<sensor>([^<]*+)</sensor>");
    private static final Pattern channelPattern = Pattern.compile("<ch([123])>\\s*+<watts>([^<]*+)</watts>\\s*+</ch\\1>");
    private static final Pattern temperaturePattern = Pattern.compile("<tmpr(F?+)>([^<]*+)</tmpr\\1>");

    private List<Triple> currentCostParse(String msg) {
        int timestamp = (int)(System.currentTimeMillis()/1000); // TODO use displayed time?

        int sensor = 0;
        int[] ch = new int[3];
        @SuppressWarnings("unused") int tmpr = 0;
        @SuppressWarnings("unused") boolean fahrenheit = false;
        @SuppressWarnings("unused") boolean hasTmpr = false;
        Matcher m = sensorPattern.matcher(msg);
        if(m.find()) {
            sensor = Integer.parseInt(m.group(1));
        }

        if(sensor > maxSensor) return null;

        List<Triple> res = null;

        m = channelPattern.matcher(msg);
        boolean found = false;
        while(m.find()) {
            found = true;
            ch[Integer.parseInt(m.group(1))-1] = Integer.parseInt(m.group(2));
        }
        if(found) {
            res = new ArrayList<Triple>();
            if(sumClamps) {
                Triple triple = new Triple(timestamp,(byte)sensor,Integer.valueOf(ch[0]+ch[1]+ch[2]),null,null);
                res.add(triple);
            }
            else {
                for(int i = 0; i < numberOfClamps; i++) {
                    Triple triple = new Triple(timestamp,(byte)(sensor*numberOfClamps+i),Integer.valueOf(ch[i]),null,null);
                    res.add(triple);
                }
            }
        }

        if(recordTemperature) {
            m = temperaturePattern.matcher(msg);
            if(m.find()) {
                hasTmpr = true;
                tmpr = Integer.parseInt(m.group(2));
                fahrenheit = "F".equals(m.group(1));
                // TODO temperature
                // if(res==null) res = new ArrayList<Triple>();
            }
        }

        return res;
    }

    private void processTriples(List<Triple> triples) {
        if(triples == null || triples.isEmpty()) return;

        List<Triple.Key> changes = new ArrayList<Triple.Key>();
        boolean changed = false;

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
            log.trace("Current Cost data at " + Util.dateString(timestamp));
            if(servlet!=null) {
                servlet.setMinimumIfNewer(timestamp);
                servlet.setMaximumIfNewer(timestamp);
            }
            catchUp.setMaximumIfNewer(timestamp);
            catchUp.notifyChanges(changes, false);

            synchronized(this) {
                long now = System.currentTimeMillis();
                if(now - latestSync > ENVIRONMENT_SYNC_INTERVAL) {
                    try {
                        databaseManager.environment.sync();
                        latestSync = now;
                        log.trace("Environment synced.");
                        if(now - latestStatsDump > STATS_DUMP_INTERVAL) {
                            log.trace(databaseManager.environment.getStats(StatsConfig.DEFAULT).toString());
                            latestStatsDump = now;
                        }
                    }
                    catch(DatabaseException e) {
                        log.debug("Exception syncing environment: " + e);
                    }
                }
            }
        }
    }

    public static void printSerialPortNames() {
        @SuppressWarnings("unchecked")
        Enumeration<CommPortIdentifier> portEnum = CommPortIdentifier.getPortIdentifiers();
        while(portEnum.hasMoreElements()) {
            CommPortIdentifier portIdentifier = portEnum.nextElement();
            if(CommPortIdentifier.PORT_SERIAL==portIdentifier.getPortType()) {
                System.out.println(portIdentifier.getName());
            }
        }        
    }
}
