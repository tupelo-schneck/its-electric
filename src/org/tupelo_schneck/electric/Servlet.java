/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

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
    Main main;
    
    public Servlet(Main main) {
        this.main = main;
    }
    
    public TimeSeriesDatabase databaseForRange(int start, int end) {
        int range = end - start;
        for (int i = 0; i < Main.durations.length - 1; i++) {
            if(Main.durations[i] * main.maxDataPoints > range) {
                if(Main.DEBUG) System.out.println("Using duration: " + Main.durations[i]); 
                return main.databases[i];
            }
        }
        return main.databases[Main.durations.length-1];
    }
    
    private static TimeZone GMT = TimeZone.getTimeZone("GMT");
    
    public static final String TIME_ZONE_OFFSET = "timeZoneOffset";
    
    private int addRowsFromIterator(DataTable data, Iterator<Triple> iter, GregorianCalendar cal) throws TypeMismatchException {
        int lastTime = 0;
        int lastMTU = 0;
        TableRow row = null;
        while(iter.hasNext()) {
            Triple triple = iter.next();
            if (triple.timestamp > lastTime) {
                if(row!=null) {
                    for(int mtu = lastMTU + 1; mtu < main.mtus; mtu++) {
                        row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
                    }
                    data.addRow(row);
                }
                row = new TableRow();
                lastTime = triple.timestamp;
                // note have to add in the time zone offset
                cal.setTimeInMillis((long)(triple.timestamp + Main.timeZoneOffset) * 1000);
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
            for(int mtu = lastMTU + 1; mtu < main.mtus; mtu++) {
                row.addCell(Value.getNullValueFromValueType(ValueType.NUMBER));
            }
            data.addRow(row);
        }
        return lastTime;
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
        
        // Create a data table,
        DataTable data = new DataTable();
        ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
        cd.add(new ColumnDescription("Date", ValueType.DATETIME, "Date"));
        for(int mtu = 0; mtu < main.mtus; mtu++) {
            String label = "MTU" + (mtu+1);
            cd.add(new ColumnDescription(label, ValueType.NUMBER, label));        
        }
        data.addColumns(cd);

        // Fill the data table.
        try {
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeZone(GMT);
            TimeSeriesDatabase bigDb = databaseForRange(main.minimum, max);
            TimeSeriesDatabase smallDb = databaseForRange(start,end);
            int lastTime = addRowsFromIterator(data, bigDb.read(main.minimum,start*2-end-1),cal);
            int nextTime = addRowsFromIterator(data, smallDb.read(start*2-end,end*2-start),cal);
            if(nextTime > 0) lastTime = nextTime;
            nextTime = addRowsFromIterator(data, bigDb.read(end*2-start+1,max),cal);
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

        // send time zone info so that client can adjust (Annotated Time Line bug)
        data.setCustomProperty(TIME_ZONE_OFFSET, String.valueOf(Main.timeZoneOffset));
        return data;
    }

    @Override
    protected boolean isRestrictedAccessMode() {
      return false;
    }

    public static void startServlet(Main main) throws Exception {
        Server server = new Server(main.port);
        ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.NO_SESSIONS|ServletContextHandler.NO_SECURITY);
        root.addServlet(new ServletHolder(new Servlet(main)), "/*");
        server.start();
    }
}
