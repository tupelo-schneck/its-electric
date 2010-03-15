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

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.google.visualization.datasource.DataSourceServlet;

import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.DateTimeValue;
import com.google.visualization.datasource.datatable.value.Value;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.Query;
import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;
import com.sleepycat.je.DatabaseException;

public class Servlet extends DataSourceServlet {
    private Log log = LogFactory.getLog(Servlet.class);

    Main main;
    
    public Servlet(Main main) {
        this.main = main;
    }
    
    public TimeSeriesDatabase databaseForRange(int start, int end) {
        int range = end - start;
        for (int i = 0; i < Main.durations.length - 1; i++) {
            if(Main.durations[i] * main.options.numDataPoints > range) {
                log.debug("Using duration: " + Main.durations[i]); 
                return main.databases[i];
            }
        }
        return main.databases[Main.durations.length-1];
    }
    
    public TimeSeriesDatabase databaseForResAndRange(int res, int start, int end) {
        if(res<=0) return databaseForRange(start,end);
        log.debug("Looking for resolution: " + res);
        int range = end - start;
        TimeSeriesDatabase fallback = null;
        for (int i = 0; i < Main.durations.length; i++) {
            if(Main.durations[i]>=res && Main.durations[i] * main.options.maxDataPoints > range) {
                log.debug("Using duration: " + Main.durations[i]); 
                return main.databases[i];
            }
            if(fallback==null && Main.durations[i] * main.options.numDataPoints > range) {
                log.debug("Fallback duration: " + Main.durations[i]); 
                fallback =  main.databases[i];
            }
        }
        if(fallback==null) fallback = main.databases[Main.durations.length-1];
        log.debug("Using fallback.");
        return fallback;
    }
    
    private static TimeZone GMT = TimeZone.getTimeZone("GMT");
    
    public static final String TIME_ZONE_OFFSET = "timeZoneOffset";
    public static final String RESOLUTION_STRING = "resolutionString";
    
    private int addRowsFromIterator(DataTable data, ReadIterator iter, GregorianCalendar cal) throws TypeMismatchException {
        try {
            int lastTime = 0;
            int lastMTU = 0;
            TableRow row = null;
            while(iter.hasNext()) {
                Triple triple = iter.next();
                if (triple.timestamp > lastTime || row==null) {
                    if(row!=null) {
                        for(int mtu = lastMTU + 1; mtu < main.options.mtus; mtu++) {
                            row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
                        }
                        data.addRow(row);
                    }
                    row = new TableRow();
                    lastTime = triple.timestamp;
                    // note have to add in the time zone offset
                    // this because we want it to show our local time.
                    cal.setTimeInMillis((long)triple.timestamp * 1000 + main.options.timeZone.getOffset(triple.timestamp));
                    row.addCell(new DateTimeValue(cal));
                    lastMTU = -1;
                }
                for(int mtu = lastMTU + 1; mtu < triple.mtu; mtu++) {
                    row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
                }
                row.addCell(triple.power);
                lastMTU = triple.mtu;
            }
            if(row!=null) {
                for(int mtu = lastMTU + 1; mtu < main.options.mtus; mtu++) {
                    row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
                }
                data.addRow(row);
            }
            return lastTime;
        }
        finally {
            iter.close();
        }
    }
    
    @Override
    public DataTable generateDataTable(Query query, HttpServletRequest req) throws TypeMismatchException {
        int max = main.maximum;
        String startString = req.getParameter("start");
        int start = main.minimum;
        if(startString!=null && startString.length()>0) {
            start = Integer.parseInt(startString);
        }
        String endString = req.getParameter("end");
        int end = max;
        if(endString!=null && endString.length()>0) {
            end = Integer.parseInt(endString);
        }
        String resString = req.getParameter("resolution");
        int res = -1;
        if(resString!=null && resString.length()>0) {
            res = Integer.parseInt(resString);
        }        
        
        // Create a data table,
        DataTable data = new DataTable();
        ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
        cd.add(new ColumnDescription("Date", ValueType.DATETIME, "Date"));
        for(int mtu = 0; mtu < main.options.mtus; mtu++) {
            String label = "MTU" + (mtu+1);
            cd.add(new ColumnDescription(label, ValueType.NUMBER, label));        
        }
        data.addColumns(cd);

        // Fill the data table.
        try {
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeZone(GMT);
            TimeSeriesDatabase bigDb = databaseForRange(main.minimum, max);
            TimeSeriesDatabase smallDb = databaseForResAndRange(res,start,end);
            String resolutionString = smallDb.resolutionString;
            if(res<0) resolutionString += " (auto)";
            else if(res<smallDb.resolution) resolutionString += " (capped)";
            data.setCustomProperty(RESOLUTION_STRING, resolutionString);
            int range = end - start;
            int lastTime = addRowsFromIterator(data, bigDb.read(main.minimum,start-range-1),cal);
            int nextTime = addRowsFromIterator(data, smallDb.read(start-range,end+range),cal);
            if(nextTime > 0) lastTime = nextTime;
            nextTime = addRowsFromIterator(data, bigDb.read(end+range+1,max-2*range-1),cal);
            if(nextTime > 0) lastTime = nextTime;
            nextTime = addRowsFromIterator(data, smallDb.read(max-2*range,max),cal);
            if(nextTime > 0) lastTime = nextTime;
            for(int i = Main.durations.length - 1; i >= 0; i--) {
                if(lastTime>=max) break;
                nextTime = addRowsFromIterator(data,main.databases[i].read(lastTime+1,max),cal);
                if(nextTime > 0) lastTime = nextTime;
            }
        }
        catch(DatabaseException e) {
            e.printStackTrace();
            return null;
        }

        // send time zone info 
        // used to say "so that client can adjust (Annotated Time Line bug)" but that was wrong;
        // the bug is about the client's time zone.
        // We'll still send this in case it's useful.  Whether it says standard or daylight time
        // is determined by the highest date in the visible range.
        data.setCustomProperty(TIME_ZONE_OFFSET, String.valueOf(main.options.timeZone.getOffset(max) / 1000));
        return data;
    }

    @Override
    protected boolean isRestrictedAccessMode() {
      return false;
    }

    public static void startServlet(Main main) throws Exception {
        HandlerCollection handlers = new HandlerCollection();
        if(main.options.serverLogFilename!=null) {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            NCSARequestLog requestLog;
            if("stderr".equals(main.options.serverLogFilename)) {
                requestLog = new NCSARequestLog();
            }
            else {
                requestLog = new NCSARequestLog(main.options.serverLogFilename);
            }
            //requestLog.setRetainDays(90);
            requestLog.setAppend(true);
            //requestLog.setExtended(false);
            //requestLog.setLogTimeZone("GMT");
            requestLogHandler.setRequestLog(requestLog);
            handlers.addHandler(requestLogHandler);
        }
            
        ServletContextHandler root = new ServletContextHandler(handlers, "/", ServletContextHandler.NO_SESSIONS|ServletContextHandler.NO_SECURITY);
        root.addServlet(new ServletHolder(new Servlet(main)), "/*");
        
        Server server = new Server(main.options.port);
        server.setHandler(handlers);
        server.start();
    }
}
