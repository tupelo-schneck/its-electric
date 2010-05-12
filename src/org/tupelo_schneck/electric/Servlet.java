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
import java.util.Comparator;
import java.util.PriorityQueue;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ShutdownThread;
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
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    public static final String TIME_ZONE_OFFSET = "timeZoneOffset";
    public static final String RESOLUTION_STRING = "resolutionString";

    private Log log = LogFactory.getLog(Servlet.class);

    private Main main;

    public Servlet(Main main) {
        this.main = main;
    }

    public TimeSeriesDatabase databaseForResolution(int res,boolean less) {
        TimeSeriesDatabase lastDb = main.secondsDb;
        for(TimeSeriesDatabase db : main.databases) {
            if(db.resolution==res) return db;
            else if(db.resolution>res) {
                return less ? lastDb : db;
            }
            lastDb = db;
        }
        return main.databases[Main.numDurations-1];
    }

    public TimeSeriesDatabase rangeDb(QueryParameters params) {
        int range = params.rangeEnd - params.rangeStart;
        TimeSeriesDatabase db;
        if(params.minPoints > 0) {
            db = databaseForResolution(range / params.minPoints, true);
        }
        else {
            db = databaseForResolution(range / params.maxPoints, false);
        }
        if(60*60 > db.resolution) {
            db = databaseForResolution(60*60, false);
        }

        return db;
    }

    public TimeSeriesDatabase zoomDb(QueryParameters params) {
        int range = params.end - params.start;
        int res = params.resolution;
        TimeSeriesDatabase db = null;
        if(res < 0) {
            if(params.minPoints > 0) {
                db = databaseForResolution(range / params.minPoints, true);
            }
            else {
                db = databaseForResolution(range / params.maxPoints, false);
            }
            res = db.resolution;
        }
        if(range / main.options.maxDataPoints > res) {
            db = databaseForResolution(range / main.options.maxDataPoints, false);
        }
        if(db==null) {
            db = databaseForResolution(res,false);
        }

        return db;
    }

    private TimeSeriesDatabase _dayDb;
    private TimeSeriesDatabase _weekDb;
    public TimeSeriesDatabase dayDb() {
        if(_dayDb==null) _dayDb = databaseForResolution(15*60,false);
        return _dayDb;
    }
    public TimeSeriesDatabase weekDb() {
        if(_weekDb==null) _weekDb = databaseForResolution(60*60,false);
        return _weekDb;
    }
    
    public static final Comparator<TableRow> TABLE_ROW_COMPARATOR = new Comparator<TableRow>() {
        @Override
        public int compare(TableRow o1, TableRow o2) {
            return o1.getCell(0).getValue().compareTo(o2.getCell(0).getValue());
        }
    };
    
    private class DataTableBuilder {
        private PriorityQueue<TableRow> rows;
        private int min = Integer.MAX_VALUE;
        private int max = 0;

        private DataTable data;
        
        private TableRow row;
        private int lastTime;
        private int lastMTU;
        private GregorianCalendar cal;
        
        public DataTableBuilder() {
            rows = new PriorityQueue<TableRow>(100,TABLE_ROW_COMPARATOR);
            cal = new GregorianCalendar(GMT);
            
            data = new DataTable();
            ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
            cd.add(new ColumnDescription("Date", ValueType.DATETIME, "Date"));
            for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                String label = "MTU" + (mtu+1);
                cd.add(new ColumnDescription(label, ValueType.NUMBER, label));        
            }
            data.addColumns(cd);
        }
        
        private void addNullsTo(int nextMTU) {
            for(int mtu = lastMTU + 1; mtu < nextMTU; mtu++) {
                row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
            }
        }
        
        private void addRow() {
            rows.add(row);
        }
        
        private void finishRow() {
            if(row!=null) {
                addNullsTo(main.options.mtus);
                addRow();
                row = null;
                lastTime = 0;
                lastMTU = 0;
            }
        }
        
        private void addTriple(Triple triple) {
            if (triple.timestamp < lastTime) return;
            if (triple.timestamp > lastTime || row==null) {
                finishRow();
                row = new TableRow();
                lastTime = triple.timestamp;
                
                if(triple.timestamp < min) min = triple.timestamp;
                if(triple.timestamp > max) max = triple.timestamp;
                
                // note have to add in the time zone offset
                // this because we want it to show our local time.
                cal.setTimeInMillis((long)triple.timestamp * 1000 + main.options.timeZone.getOffset((long)triple.timestamp*1000));
                row.addCell(new DateTimeValue(cal));
                lastMTU = -1;
            }
            addNullsTo(triple.mtu);
            row.addCell(triple.power);
            lastMTU = triple.mtu;
        }
        
        /* returns whether any rows were in fact added */
        public boolean addRowsFromIterator(ReadIterator iter) {
            boolean res = false;
            int priorMin = min;
            int priorMax = max;
            try {
                while(iter.hasNext()) {
                    Triple triple = iter.next();
                    if(triple.timestamp >= priorMin && triple.timestamp <= priorMax) continue;
                    addTriple(triple);
                    res = true;
                }
            }
            finally {
                iter.close();
            }
            finishRow();
            return res;
        }
        
        public void addOneRowFromIterator(ReadIterator iter) {
            int priorMin = min;
            int priorMax = max;
            int time = 0;
            try {
                while(iter.hasNext()) {
                    Triple triple = iter.next();
                    if(time == 0) {
                        time = triple.timestamp;
                    }
                    else if (triple.timestamp!=time) break;
                    if(triple.timestamp >= priorMin && triple.timestamp <= priorMax) break;
                    addTriple(iter.next());
                }
            }
            finally {
                iter.close();
            }
            finishRow();
        }
        
        public int min() {
            return min;
        }
        
        public int max() {
            return max;
        }
        
        public void setCustomProperty(String key, String value) {
            data.setCustomProperty(key, value);
        }
        
        public DataTable dataTable() {
            try {
                TableRow nextRow;
                while((nextRow = rows.poll()) != null) {
                    data.addRow(nextRow);
                }
            }
            catch(TypeMismatchException e) {
                throw new RuntimeException(e);
            }
            return data;
        }
    }
    
    private class QueryParameters {
        public int rangeStart;
        public int rangeEnd;
        public int start;
        public int end;
        public int resolution;
        public int minPoints;
        public int maxPoints;
        public int extraPoints;
        
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
        
        public QueryParameters(HttpServletRequest req,int max) {
            this.req = req;
            
            rangeStart = getIntParameter("rangeStart",main.minimum);
            rangeEnd = getIntParameter("rangeEnd",max);
            start = getIntParameter("start",rangeStart);
            end = getIntParameter("end",rangeEnd);

            boolean realTimeAdjust = req.getParameter("realTimeAdjust") != null;
            if(realTimeAdjust) {
                int range = rangeEnd - rangeStart;
                rangeEnd = max;
                rangeStart = rangeEnd - range;
                int zoomedRange = end - start;
                end = max;
                start = end - zoomedRange;
            }

            resolution = getIntParameter("resolution",-1);
            minPoints = getIntParameter("minPoints",-1);
            maxPoints = getIntParameter("maxPoints",-1);
            if(minPoints<=0 && maxPoints<=0) maxPoints = main.options.numDataPoints;
            
            extraPoints = getIntParameter("extraPoints",0);
        }
    }
    
    @Override
    public DataTable generateDataTable(Query query, HttpServletRequest req) {
        int max = main.maximum;
        QueryParameters params = new QueryParameters(req,max);

        // Create a data table,
        DataTableBuilder builder = new DataTableBuilder();

        // Fill the data table.
        try {
            TimeSeriesDatabase zoomDb = zoomDb(params);
            TimeSeriesDatabase rangeDb = rangeDb(params);
            if(rangeDb.resolution < zoomDb.resolution) rangeDb = zoomDb; 

            String resolutionString = zoomDb.resolutionString;
            if(params.resolution<0) resolutionString += " (auto)";
            else if(params.resolution<zoomDb.resolution) resolutionString += " (capped)";
            builder.setCustomProperty(RESOLUTION_STRING, resolutionString);
            
            int range = params.end - params.start;
            int start = params.extraPoints > 1 ? Math.max(params.rangeStart, params.start - range) : params.start;
            int end = params.extraPoints > 1 ? Math.min(params.rangeEnd, params.end + range) : params.end;
            builder.addRowsFromIterator(zoomDb.read(start,end));
            
            int origEnd = end;
            
            int rangeEnd = params.rangeEnd;
            if(params.extraPoints > 1) {
                if(end < params.rangeEnd) {
                    rangeEnd = Math.max(end, params.rangeEnd - range - 1);
                }
                if(start > params.rangeStart) {
                    TimeSeriesDatabase dayDb = dayDb();
                    if(dayDb.resolution < zoomDb.resolution) dayDb = zoomDb;
                    if(dayDb.resolution > rangeDb.resolution) dayDb = rangeDb; 
                    end = start;
                    start = Math.max(params.rangeStart, params.end - 86400); 
                    builder.addRowsFromIterator(dayDb.read(start,end - 1));
                }
                if(start > params.rangeStart) {
                    TimeSeriesDatabase weekDb = weekDb();
                    if(weekDb.resolution < zoomDb.resolution) weekDb = zoomDb;
                    if(weekDb.resolution > rangeDb.resolution) weekDb = rangeDb; 
                    end = start;
                    start = Math.max(params.rangeStart, params.end - 86400 * 8); 
                    builder.addRowsFromIterator(weekDb.read(start,end - 1));
                }
//                if(start > params.rangeStart) {
//                    TimeSeriesDatabase monthDb = monthDb();
//                    if(monthDb.resolution < zoomDb.resolution) monthDb = zoomDb;
//                    if(monthDb.resolution > rangeDb.resolution) monthDb = rangeDb; 
//                    end = start;
//                    start = Math.max(params.rangeStart, params.end - 86400 * 32); 
//                    builder.addRowsFromIterator(monthDb.read(start,end - 1));
//                }
            }
            
            if(start > params.rangeStart) {
                builder.addRowsFromIterator(rangeDb.read(params.rangeStart,start - 1));
            }
            if(origEnd < rangeEnd) {
                builder.addRowsFromIterator(rangeDb.read(origEnd + 1,rangeEnd));
            }

            if(params.extraPoints > 1 && rangeEnd < params.rangeEnd) {
                builder.addRowsFromIterator(zoomDb.read(rangeEnd+1,params.rangeEnd));
            }
            
            if(params.extraPoints > 0) {
                if(builder.min() > params.start) builder.addOneRowFromIterator(main.secondsDb.read(params.start));

                if(params.end == max) {
                    int zoomDbIndex;
                    for(zoomDbIndex = Main.numDurations - 1; zoomDbIndex >= 0; zoomDbIndex--) {
                        if(main.databases[zoomDbIndex].resolution == zoomDb.resolution) break;
                    }
                    if(zoomDbIndex > 0) {
                        int nextTime = builder.max() + main.databases[zoomDbIndex].resolution - main.databases[zoomDbIndex-1].resolution + 1;
                        log.debug("After resolution " + main.databases[zoomDbIndex].resolution + " max = " + Main.dateString(builder.max()) + " nextTime = " + Main.dateString(nextTime));
                        for(int i = zoomDbIndex - 1; i >= 1; i--) {
                            if(nextTime>=max) break;
                            if (builder.addRowsFromIterator(main.databases[i].read(nextTime,max))) {
                                nextTime = builder.max() + main.databases[i].resolution - main.databases[i-1].resolution + 1;
                                log.debug("After resolution " + main.databases[i].resolution + " max = " + Main.dateString(builder.max()) + " nextTime = " + Main.dateString(nextTime));
                            }
                        }
                        if(builder.max() < max) {
                            nextTime = Math.min(nextTime, max);
                            builder.addRowsFromIterator(main.databases[0].read(nextTime,max));
                        }
                    }
                }
                else if(builder.max() < params.end) {
                    builder.addOneRowFromIterator(main.secondsDb.read(params.end));
                }
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

        ShutdownThread.getInstance(); // work around a jetty bug that causes problems at shutdown

        Server server = new Server(main.options.port);
        server.setHandler(handlers);
        server.start();
        return server;
    }
}
