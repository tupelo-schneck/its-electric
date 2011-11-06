package org.tupelo_schneck.electric.current_cost;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gnu.io.CommPort;
import gnu.io.CommPortIdentifier;
import gnu.io.SerialPort;

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

public class CurrentCostImporter implements Importer, Runnable {
    static {
        System.out.println("RXTX version mismatch warning can safely be ignored.");
    }
    
    private Log log = LogFactory.getLog(CurrentCostImporter.class);

    /** Milliseconds to block while waiting for port open */
    private static final int TIME_OUT = 5000;
    private static final int[] SERIAL_SPEEDS = new int[] { 57600, 9600, 2400 }; 
    private static final int MAX_FAILS = 5;
    private static final long STATS_DUMP_INTERVAL = 5*60*1000;

    private final Main main;
    private final DatabaseManager databaseManager;
    private final Options options;
    private final Servlet servlet;
    private final CatchUp catchUp;

    private String portName;
    private int maxSensors;
    private int numberOfClamps;
    private boolean sumClamps;
    private boolean recordTemperature;

    private SerialPort serialPort;
    private boolean modelVerified;
    private int serialSpeed;
    private InputStream input;
    private Reader reader;
    private ExecutorService execServ = Executors.newSingleThreadExecutor();
    private int fails;

    private long latestStatsDump;

    public CurrentCostImporter(Main main, Options options, DatabaseManager databaseManager, Servlet servlet, CatchUp catchUp) {
        this.main = main;
        this.databaseManager = databaseManager;
        this.options = options;
        this.servlet = servlet;
        this.catchUp = catchUp;
        
        // TODO options
        portName = this.options.ccPortName;
        maxSensors = 0;
        numberOfClamps = 3;
        sumClamps = true;
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
            reader = new BufferedReader(new InputStreamReader(input,"UTF-8"));
            execServ.execute(this);
        }
        catch(Exception e) {
            e.printStackTrace();
            main.shutdown();
            return;
        }
    }

    private synchronized void closeSerial() {
        if (serialPort != null) {
            serialPort.close();
        }
    }

    @Override
    public synchronized void shutdown() {
        execServ.shutdownNow();
        closeSerial();
    }

    @Override
    public void run() {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[4096];
        while(main.isRunning) {
            if(fails > MAX_FAILS) {
                log.error("Too many errors reading from serial port.");
                main.shutdown();
                return;
            }

            int r;
            try {
                r = reader.read(buf);
            }
            catch(IOException e) {
                fails++;
                sb.delete(0,sb.length());
                log.error("Error reading from serial port", e);
                continue;
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

            if (!modelVerified) {    
                boolean nonsenseXML = false;
                for(int i = 0; i < r; i++) {
                    char ch = buf[i];
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

            sb.append(buf,0,r);
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
    }

    private static final Pattern sensorPattern = Pattern.compile("<sensor>([^<]*+)</sensor>");
    private static final Pattern channelPattern = Pattern.compile("<ch([123])>\\s*+<watts>([^<]*+)</watts>\\s*+</ch\\1>");
    private static final Pattern temperaturePattern = Pattern.compile("<tmpr(F?+)>([^<]*+)</tmpr\\1>");

    private List<Triple> currentCostParse(String msg){
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

        if(sensor > maxSensors) return null;

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

            try {
                // TODO throttle
                databaseManager.environment.sync();
                log.trace("Environment synced.");
                long now = System.currentTimeMillis();
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
//    
//    
//    public CurrentCostImporter() {
//        this.main = null;
//        this.databaseManager = null;
//        this.options = null;
//        this.servlet = null;
//        this.catchUp = null;
//        
//        // TODO options
//        portName = null;
//        maxSensors = 0;
//        numberOfClamps = 3;
//        sumClamps = true;
//        recordTemperature = false;
//    }
//
//    public static void main(String[] args) {
//        String msg = "<msg>\n   <src>CC128-v0.11</src>\n   <dsb>00089</dsb>\n   <time>13:02:39</time>\n   <tmpr>18.7</tmpr>\n   <sensor>0</sensor>\n   <id>01234</id>\n   <type>1</type>\n   <ch1>\n      <watts>00345</watts>\n   </ch1>\n   <ch2>\n      <watts>02151</watts>\n   </ch2>\n   <ch3>\n      <watts>00000</watts>\n   </ch3>\n</msg>\n";
//        msg = "<msg>\n  <date>\n    <dsb>00030</dsb>\n    <hr>00</hr><min>20</min><sec>11</sec>\n  </date>\n  <src>\n    <name>CC02</name>\n    <id>00077</id> \n    <type>1</type>\n    <sver>1.06</sver>\n  </src>\n  <ch1>\n    <watts>00168</watts>\n  </ch1>\n  <ch2>\n    <watts>00000</watts>\n  </ch2>\n  <ch3>\n    <watts>00000</watts>\n  </ch3>\n  <tmpr>25.6</tmpr>\n  <hist>\n    <hrs>\n      <h02>000.3</h02>\n      ....\n      <h26>003.1</h26>\n    </hrs>\n    <days>\n\n      <d01>0014</d01>\n      ....\n      <d31>0000</d31>\n    </days>\n    <mths>\n\n      <m01>0000</m01>\n      ....\n      <m12>0000</m12>\n    </mths>\n    <yrs>\n\n      <y1>0000000</y1>\n      ....\n      <y4>0000000</y4>\n    </yrs>\n  </hist>\n</msg>\n";
//        System.out.println(new CurrentCostImporter().currentCostParse(msg));
//    }
}
