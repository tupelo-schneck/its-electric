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
        for (int i = 0; i < Main.numDurations - 1; i++) {
            if(Main.durations[i] * main.options.numDataPoints > range) {
                log.debug("Using duration: " + Main.durations[i]); 
                return main.databases[i];
            }
        }
        return main.databases[Main.numDurations-1];
    }

    public TimeSeriesDatabase databaseForResAndRange(int res, int start, int end) {
        if(res<=0) return databaseForRange(start,end);
        log.debug("Looking for resolution: " + res);
        int range = end - start;
        TimeSeriesDatabase fallback = null;
        for (int i = 0; i < Main.numDurations; i++) {
            if(Main.durations[i]>=res && Main.durations[i] * main.options.maxDataPoints > range) {
                log.debug("Using duration: " + Main.durations[i]); 
                return main.databases[i];
            }
            if(fallback==null && Main.durations[i] * main.options.numDataPoints > range) {
                log.debug("Fallback duration: " + Main.durations[i]); 
                fallback =  main.databases[i];
            }
        }
        if(fallback==null) fallback = main.databases[Main.numDurations-1];
        log.debug("Using fallback.");
        return fallback;
    }

    private static TimeZone GMT = TimeZone.getTimeZone("GMT");

    public static final String TIME_ZONE_OFFSET = "timeZoneOffset";
    public static final String RESOLUTION_STRING = "resolutionString";

    private class DataTableBuilder {
        private DataTable data;
        private int lastTime;
        private TableRow row;
        private int lastMTU;
        private GregorianCalendar cal;
        
        private DataTableBuilder() {
            data = new DataTable();
            ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
            cd.add(new ColumnDescription("Date", ValueType.DATETIME, "Date"));
            for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                String label = "MTU" + (mtu+1);
                cd.add(new ColumnDescription(label, ValueType.NUMBER, label));        
            }
            data.addColumns(cd);
            
            cal = new GregorianCalendar(GMT);
        }
        
        private void addNullsTo(int nextMTU) {
            for(int mtu = lastMTU + 1; mtu < nextMTU; mtu++) {
                row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
            }
        }
        
        private void addRow() {
            try { data.addRow(row); } catch (TypeMismatchException e) { throw new RuntimeException(e); }
        }
        
        private void finishRow() {
            if(row!=null) {
                addNullsTo(main.options.mtus);
                addRow();
            }
        }
        
        private void addTriple(Triple triple) {
            if (triple.timestamp < lastTime) return;
            if (triple.timestamp > lastTime || row==null) {
                finishRow();
                row = new TableRow();
                lastTime = triple.timestamp;
                // note have to add in the time zone offset
                // this because we want it to show our local time.
                cal.setTimeInMillis((long)triple.timestamp * 1000 + main.options.timeZone.getOffset((long)triple.timestamp*1000));
                row.addCell(new DateTimeValue(cal));
                lastMTU = -1;
            }
            addNullsTo(triple.mtu);
            for(int mtu = lastMTU + 1; mtu < triple.mtu; mtu++) {
                row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
            }
            row.addCell(triple.power);
            lastMTU = triple.mtu;
        }
        
        public void addRowsFromIterator(ReadIterator iter) {
            try {
                while(iter.hasNext()) {
                    addTriple(iter.next());
                }
            }
            finally {
                iter.close();
            }
        }
        
        public int lastTime() {
            return lastTime;
        }
        
        public void setCustomProperty(String key, String value) {
            data.setCustomProperty(key, value);
        }
        
        public DataTable dataTable() {
            finishRow();
            return dataTable();
        }
    }
    
    private class QueryParameters {
        public int start = main.maximum;
        public int end = main.minimum;
        public int resolution = -1;
        
        private HttpServletRequest req;

        private int getIntParameter(String name,int def) {
            int res = def;
            String param = req.getParameter(name);
            if(param!=null && param.length()>0) {
                try {
                    res = Integer.parseInt(param);
                }
                catch(NumberFormatException e) { log.error("Error parsing " + name,e); }
            }
            return res;
        }
        
        public QueryParameters(HttpServletRequest req) {
            this.req = req;

            start = getIntParameter("start",main.minimum);
            end = getIntParameter("end",main.maximum);
            resolution = getIntParameter("resolution",-1);
        }
    }
    
    @Override
    public DataTable generateDataTable(Query query, HttpServletRequest req) {
        int max = main.maximum;

        QueryParameters params = new QueryParameters(req);

        // Create a data table,
        DataTableBuilder builder = new DataTableBuilder();

        // Fill the data table.
        try {
            TimeSeriesDatabase bigDb = databaseForRange(main.minimum, max);
            TimeSeriesDatabase smallDb = databaseForResAndRange(params.resolution,params.start,params.end);
            
            String resolutionString = smallDb.resolutionString;
            if(params.resolution<0) resolutionString += " (auto)";
            else if(params.resolution<smallDb.resolution) resolutionString += " (capped)";
            builder.setCustomProperty(RESOLUTION_STRING, resolutionString);
            
            int range = params.end - params.start;
            
            log.trace("Reading " + Main.dateString(main.minimum) + " to " + Main.dateString(params.start-range-1) + " at " + bigDb.resolutionString);
            builder.addRowsFromIterator(bigDb.read(main.minimum,params.start-range-1));
            log.trace("Reading " + Main.dateString(params.start-range) + " to " + Main.dateString(params.end+range) + " at " + smallDb.resolutionString);
            builder.addRowsFromIterator(smallDb.read(params.start-range,params.end+range));
            log.trace("Reading " + Main.dateString(params.end+range+1) + " to " + Main.dateString(max-2*range-1) + " at " + bigDb.resolutionString);
            builder.addRowsFromIterator(bigDb.read(params.end+range+1,max-2*range-1));
            log.trace("Reading " + Main.dateString(max-2*range) + " to " + Main.dateString(max) + " at " + smallDb.resolutionString);
            builder.addRowsFromIterator(smallDb.read(max-2*range,max));
            for(int i = Main.numDurations - 1; i >= 0; i--) {
                int lastTime = builder.lastTime();
                if(lastTime>=max) break;
                log.trace("Reading " + Main.dateString(lastTime+1) + " to " + Main.dateString(max) + " at " + main.databases[i].resolutionString);
                builder.addRowsFromIterator(main.databases[i].read(lastTime+1,max));
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
        builder.setCustomProperty(TIME_ZONE_OFFSET, String.valueOf(main.options.timeZone.getOffset(1000L*params.end) / 1000));
        return builder.dataTable();
    }

    @Override
    protected boolean isRestrictedAccessMode() {
        return false;
    }

    public static Server startServlet(Main main) throws Exception {
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
        return server;
    }
}
