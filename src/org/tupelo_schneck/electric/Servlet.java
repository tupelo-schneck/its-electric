/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2011 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.google.visualization.datasource.DataSourceServlet;

import com.google.visualization.datasource.base.DataSourceException;
import com.google.visualization.datasource.base.ReasonType;
import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.DateTimeValue;
import com.google.visualization.datasource.datatable.value.Value;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.Query;
import com.ibm.icu.util.GregorianCalendar;
import com.sleepycat.je.DatabaseException;

public class Servlet extends DataSourceServlet {
    public static final String TIME_ZONE_OFFSET = "timeZoneOffset";
    public static final String RESOLUTION_STRING = "resolutionString";
    public static final String RESOLUTION = "resolution";
    public static final String MINIMUM_STRING = "minimum";
    public static final String MAXIMUM_STRING = "maximum";
    
    private Log log = LogFactory.getLog(Servlet.class);

    private final Main main;
    private final DatabaseManager databaseManager;

    public Servlet(Main main, DatabaseManager databaseManager) {
        this.main = main;
        this.databaseManager = databaseManager;
    }

    public TimeSeriesDatabase databaseForResolution(int res,boolean less) {
        TimeSeriesDatabase lastDb = databaseManager.secondsDb;
        for(TimeSeriesDatabase db : databaseManager.databases) {
            if(db.resolution==res) return db;
            else if(db.resolution>res) {
                return less ? lastDb : db;
            }
            lastDb = db;
        }
        return databaseManager.databases[DatabaseManager.numDurations-1];
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
    
    private static final Value NULL_NUMBER = Value.getNullValueFromValueType(ValueType.NUMBER);

    private class DataTableBuilder {
        private QueryParameters params;
        
        private PriorityQueue<TableRow> rows;
        private int min = Integer.MAX_VALUE;
        private int max = 0;

        private DataTable data;
        
        private TableRow row;
        private int lastTime;
        private int lastMTU;
        private GregorianCalendar cal;
        
        public DataTableBuilder(QueryParameters params) {
            this.params = params;
            rows = new PriorityQueue<TableRow>(100,TABLE_ROW_COMPARATOR);
            cal = new GregorianCalendar(Util.GMT);
            
            data = new DataTable();
            ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
            cd.add(new ColumnDescription("Date", ValueType.DATETIME, "Date"));
            for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                String label = "MTU" + (mtu+1);
                cd.add(new ColumnDescription(label, ValueType.NUMBER, label));
            }
            if(params.queryType==QueryType.COMBINED_POWER) {
                for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                    String label = "MTU" + (mtu+1) + "var";
                    cd.add(new ColumnDescription(label, ValueType.NUMBER, label));
                }
                for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                    String label = "MTU" + (mtu+1) + "VA";
                    cd.add(new ColumnDescription(label, ValueType.NUMBER, label));
                }
            }
            data.addColumns(cd);
        }
        
        private void addNullsTo(int nextMTU) {
            for(int mtu = lastMTU + 1; mtu < nextMTU; mtu++) {
                row.addCell(NULL_NUMBER);
                if(params.queryType==QueryType.COMBINED_POWER) {
                    row.addCell(NULL_NUMBER);
                    row.addCell(NULL_NUMBER);
                }
            }
        }
        
        private void addRow() {
            if(params.queryType==QueryType.COMBINED_POWER) {
                TableRow oldRow = row;
                row = new TableRow();
                row.addCell(oldRow.getCell(0));
                for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                    row.addCell(oldRow.getCell(1+mtu*3));
                }
                for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                    row.addCell(oldRow.getCell(2+mtu*3));
                }
                for(int mtu = 0; mtu < main.options.mtus; mtu++) {
                    row.addCell(oldRow.getCell(3+mtu*3));
                }
            }
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
            if(triple.mtu >= main.options.mtus) return;
            if(triple.timestamp < lastTime) return;
            if(params.queryType==QueryType.VOLTAGE && triple.voltage==null) return;
            else if(params.queryType==QueryType.POWER && triple.power==null) return;
            else if(params.queryType==QueryType.VOLT_AMPERES && triple.voltAmperes==null) return;
            else if(params.queryType==QueryType.VOLT_AMPERES_REACTIVE && (triple.voltAmperes==null || triple.power==null)) return;
            else if(params.queryType==QueryType.COMBINED_POWER && triple.voltAmperes==null && triple.power==null) return;
            else if(params.queryType==QueryType.POWER_FACTOR && (triple.voltAmperes==null || triple.power==null || triple.voltAmperes.intValue()==0)) return;
            if(triple.timestamp > lastTime || row==null) {
                finishRow();
                row = new TableRow();
                lastTime = triple.timestamp;
                
                if(triple.timestamp < min) min = triple.timestamp;
                if(triple.timestamp > max) max = triple.timestamp;
                
                // note have to add in the time zone offset
                // this because we want it to show our local time.
                cal.setTimeInMillis((long)triple.timestamp * 1000 + main.options.serveTimeZone.getOffset((long)triple.timestamp*1000));
                row.addCell(new DateTimeValue(cal));
                lastMTU = -1;
            }
            addNullsTo(triple.mtu);
            if(params.queryType==QueryType.VOLTAGE) {
                if(triple.voltage==null) row.addCell(NULL_NUMBER);
                else row.addCell((double)triple.voltage.intValue()/20);
            }
            else if(params.queryType==QueryType.POWER) {
                if(triple.power==null) row.addCell(NULL_NUMBER);
                else row.addCell(triple.power.intValue());
            }
            else if(params.queryType==QueryType.VOLT_AMPERES) {
                if(triple.voltAmperes==null) row.addCell(NULL_NUMBER);
                else row.addCell(triple.voltAmperes.intValue());
            }
            else if(params.queryType==QueryType.VOLT_AMPERES_REACTIVE) {
                if(triple.power==null || triple.voltAmperes==null) row.addCell(NULL_NUMBER);
                else {
                    double w = triple.power.intValue();
                    double va = triple.voltAmperes.intValue();
                    double varsqr = va*va - w*w;
                    if(varsqr < 0) row.addCell(0);
                    else row.addCell(Math.round(Math.sqrt(varsqr)));
                }
            }
            else if(params.queryType==QueryType.COMBINED_POWER) {
                if(triple.power==null) row.addCell(NULL_NUMBER);
                else row.addCell(triple.power.intValue());
                if(triple.power==null || triple.voltAmperes==null) row.addCell(NULL_NUMBER);
                else {
                    double w = triple.power.intValue();
                    double va = triple.voltAmperes.intValue();
                    double varsqr = va*va - w*w;
                    if(varsqr < 0) row.addCell(0);
                    else row.addCell(Math.round(Math.sqrt(varsqr)));
                }
                if(triple.voltAmperes==null) row.addCell(NULL_NUMBER);
                else row.addCell(triple.voltAmperes.intValue());
            }
            else if(params.queryType==QueryType.POWER_FACTOR) {
                if(triple.power==null || triple.voltAmperes==null || triple.voltAmperes.intValue()==0) row.addCell(NULL_NUMBER);
                else {
                    double factor = (triple.power.intValue() * 1000 / triple.voltAmperes.intValue()) / 1000.0;
                    if(factor > 1.0) factor = 1.0;
                    if(factor < -1.0) factor = -1.0;
                    row.addCell(factor);
                }
            }
            lastMTU = triple.mtu;
        }
        
        /* returns whether any rows were in fact added */
        public boolean addRowsFromIterator(ReadIterator iter) {
            return addRowsFromIterator(iter,0);
        }
        
        /* returns whether any rows were in fact added */
        public boolean addRowsFromIterator(ReadIterator iter, int limit) {
            boolean res = false;
            int priorMin = min;
            int priorMax = max;
            int time = 0;
            int count = 0;
            try {
                while(iter.hasNext()) {
                    Triple triple = iter.next();
                    if(triple.timestamp >= priorMin && triple.timestamp <= priorMax) continue;
                    if(limit > 0) {
                        if(time!=triple.timestamp) count++;
                        if(count > limit) break;
                        time = triple.timestamp;
                    }
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
    
    private enum QueryType {
        POWER, VOLTAGE, VOLT_AMPERES, POWER_FACTOR, VOLT_AMPERES_REACTIVE, COMBINED_POWER;
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
        public QueryType queryType;
        
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
        
        private int getTimestampParameter(String name,boolean isEnd,int def) {
            int res = def;
            String param = req.getParameter(name);
            if(param!=null && param.length()>0) {
                try {
                    res = Util.timestampFromUserInput(param,isEnd,main.options.serveTimeZone);
                }
                catch(NumberFormatException e) { log.error("Error parsing " + name,e); }
            }
            return res;
        }
        
        public QueryParameters(HttpServletRequest req,int min,int max) throws DataSourceException {
            this.req = req;
            
            String path = req.getPathInfo();
            if(path==null || "".equals(path) || "/".equals(path)) path = "power";
            else if(path.startsWith("/")) path = path.substring(1);
            
            if(path.equals("power")) queryType = QueryType.POWER;
            else if(path.equals("voltage")) queryType = QueryType.VOLTAGE;
            else if(path.equals("volt-amperes")) queryType = QueryType.VOLT_AMPERES;
            else if(path.equals("volt-amperes-reactive")) queryType = QueryType.VOLT_AMPERES_REACTIVE;
            else if(path.equals("combined-power")) queryType = QueryType.COMBINED_POWER;
            else if(path.equals("power-factor")) queryType = QueryType.POWER_FACTOR;
            else {
                throw new DataSourceException(ReasonType.INVALID_REQUEST, "Request '" + path + "' unknown");
            }
//            if(queryType==QueryType.VOLTAGE) {
//                if(!main.options.voltage) throw new DataSourceException(ReasonType.INVALID_REQUEST, "Voltage data not available");            
//            }
//            else if(queryType != QueryType.POWER) {
//                if(main.options.voltAmpereImportIntervalMS==0) throw new DataSourceException(ReasonType.INVALID_REQUEST, "Volt-amperage data not available");
//            }
            
            rangeStart = getTimestampParameter("rangeStart",false,min);
            rangeEnd = getTimestampParameter("rangeEnd",true,max);
            start = getTimestampParameter("start",false,rangeStart);
            end = getTimestampParameter("end",true,rangeEnd);

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
            
            String extraPointsString = req.getParameter("extraPoints");
            if("yes".equals(extraPointsString)) extraPoints = 2;
            else extraPoints = getIntParameter("extraPoints",0);
        }
    }
    
    @Override
    public DataTable generateDataTable(Query query, HttpServletRequest req) throws DataSourceException {
        int min = main.minimum;
        int max = main.maximum;
        
        QueryParameters params = new QueryParameters(req,min,max);
        
        log.trace("Begin query for " + params.queryType);
        
        // Create a data table,
        DataTableBuilder builder = new DataTableBuilder(params);

        // Fill the data table.
        boolean possibleRedraw = false; 
        try {
            TimeSeriesDatabase zoomDb = zoomDb(params);
            TimeSeriesDatabase rangeDb = rangeDb(params);
            if(rangeDb.resolution < zoomDb.resolution) rangeDb = zoomDb; 
            
            possibleRedraw = params.end == max && zoomDb.resolution <= 60;

            String resolutionString = zoomDb.resolutionString;
            if(params.resolution<0) resolutionString += " (auto)";
            else if(params.resolution<zoomDb.resolution) resolutionString += " (capped)";
            builder.setCustomProperty(RESOLUTION_STRING, resolutionString);
            builder.setCustomProperty(RESOLUTION,String.valueOf(zoomDb.resolution));
            
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
                if(builder.min() > params.start) {
                    builder.addRowsFromIterator(databaseManager.secondsDb.read(params.start),1);
                }

                if(params.end == max && builder.max()>0 && !possibleRedraw) {
                    int zoomDbIndex;
                    for(zoomDbIndex = DatabaseManager.numDurations - 1; zoomDbIndex >= 0; zoomDbIndex--) {
                        if(databaseManager.databases[zoomDbIndex].resolution == zoomDb.resolution) break;
                    }
                    if(zoomDbIndex > 0) {
                        int nextTime = builder.max() + databaseManager.databases[zoomDbIndex].resolution - databaseManager.databases[zoomDbIndex-1].resolution + 1;
                        log.debug("After resolution " + databaseManager.databases[zoomDbIndex].resolution + " max = " + Util.dateString(builder.max()) + " nextTime = " + Util.dateString(nextTime));
                        for(int i = zoomDbIndex - 1; i >= 1; i--) {
                            if(nextTime>=max) break;
                            if (builder.addRowsFromIterator(databaseManager.databases[i].read(nextTime,max),10)) {
                                nextTime = builder.max() + databaseManager.databases[i].resolution - databaseManager.databases[i-1].resolution + 1;
                                log.debug("After resolution " + databaseManager.databases[i].resolution + " max = " + Util.dateString(builder.max()) + " nextTime = " + Util.dateString(nextTime));
                            }
                        }
                        if(builder.max() < max) {
                            nextTime = Math.min(nextTime, max);
                            builder.addRowsFromIterator(databaseManager.databases[0].read(nextTime,max),10);
                        }
                    }
                }
                else if(builder.max() < params.end && !possibleRedraw) {
                    builder.addRowsFromIterator(databaseManager.secondsDb.read(params.end),1);
                }
            }
        }
        catch(DatabaseException e) {
            e.printStackTrace();
            throw new DataSourceException(ReasonType.INTERNAL_ERROR, e.getMessage());
        }

        // These return the timestamp where UTC clock shows what would be local time
        builder.setCustomProperty(MINIMUM_STRING, String.valueOf(min + main.options.serveTimeZone.getOffset(1000L*min)/1000));
        int sentMax = possibleRedraw ? builder.max() : max;
        builder.setCustomProperty(MAXIMUM_STRING, String.valueOf(sentMax + main.options.serveTimeZone.getOffset(1000L*sentMax)/1000));
        
        // send time zone info 
        // used to say "so that client can adjust (Annotated Time Line bug)" but that was wrong;
        // the bug is about the client's time zone.
        // We'll still send this in case it's useful.  Whether it says standard or daylight time
        // is determined by the highest date in the visible range.
        builder.setCustomProperty(TIME_ZONE_OFFSET, String.valueOf(main.options.serveTimeZone.getOffset(1000L*params.end) / 1000));
        log.trace("Query complete.");
        return builder.dataTable();
    }

    @Override
    protected boolean isRestrictedAccessMode() {
        return false;
    }

    public static Server setupServlet(Main main, DatabaseManager databaseManager) throws Exception {
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
        root.addServlet(new ServletHolder(new Servlet(main,databaseManager)), "/*");

        Server server = new Server(main.options.port);
        server.setHandler(handlers);
        return server;
    }
}
