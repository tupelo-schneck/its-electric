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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;
import org.tupelo_schneck.electric.current_cost.CurrentCostImporter;
import org.tupelo_schneck.electric.ted.TedImporter;

import com.sleepycat.je.DatabaseException;

public class Main {
    private Log log = LogFactory.getLog(Main.class);

    private final Options options;
    private final DatabaseManager databaseManager;
    private Servlet servlet;
    private Importer importer;
    
    private Server server;
    private ExecutorService catchUpTask;
    private ExecutorService deleteTask;
    
    private CatchUp catchUp;

    public volatile boolean isRunning = true;

    public Main(Options options, DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
        this.options = options;
    }
    
    public void shutdown() {
        if(isRunning) {
            try { log.info("Exiting."); } catch (Throwable e) {}
            isRunning = false;
            try { deleteTask.shutdownNow(); } catch (Throwable e) {}
            try { catchUpTask.shutdownNow(); } catch (Throwable e) {}
            try { importer.shutdown(); } catch(Throwable e) {}
            try { deleteTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            //try { synchronized(newDataLock) { newDataLock.notifyAll(); } } catch (Throwable e) {}
            try { catchUpTask.awaitTermination(60, TimeUnit.SECONDS); } catch (Throwable e) {}
            try { server.stop(); } catch (Throwable e) {}
            try { databaseManager.close(); } catch (Throwable e) {}
        }
    }
    
    private void export() throws DatabaseException {
        TimeSeriesDatabase database = databaseManager.databases[DatabaseManager.numDurations - 1];
        for(int i = DatabaseManager.numDurations - 2; i >= 0; i--) {
            if(database.resolution <= options.resolution) break;
            database = databaseManager.databases[i];
        }
        
        ReadIterator iter = database.read(options.startTime,options.endTime);
        try {
            if(options.exportByMTU) {
                while(iter.hasNext() && isRunning) {
                    Triple triple = iter.next();
                    System.out.print(Util.dateString(triple.timestamp));
                    System.out.print(",");
                    System.out.print(triple.mtu + 1);
                    System.out.print(",");
                    if(triple.power!=null) System.out.print(triple.power);
                    System.out.print(",");
                    if(triple.voltage!=null) System.out.print((double)triple.voltage.intValue()/20);
                    System.out.print(",");
                    if(triple.voltAmperes!=null) System.out.print(triple.voltAmperes);
                    System.out.println();
                }
            }
            else {
                int lastTime = 0;
                int lastMTU = -1;
                StringBuilder row = new StringBuilder();
                while(iter.hasNext() && isRunning) {
                    Triple triple = iter.next();
                    if(triple.timestamp < lastTime) continue;
                    if(triple.timestamp > lastTime) {
                        if(row.length()>0) {
                            for(int i = lastMTU + 1; i < options.mtus; i++) {
                                row.append(",,,");
                            }
                            System.out.println(row);
                            row.delete(0,row.length());
                        }
                        row.append(Util.dateString(triple.timestamp));
                        lastTime = triple.timestamp;
                        lastMTU = -1;
                    }
                    for(int i = lastMTU + 1; i < triple.mtu; i++) {
                        row.append(",,,");
                    }
                    lastMTU = triple.mtu;
                    row.append(",");
                    if(triple.power!=null) row.append(triple.power);
                    row.append(",");
                    if(triple.voltage!=null) row.append((double)triple.voltage.intValue()/20);
                    row.append(",");
                    if(triple.voltAmperes!=null) row.append(triple.voltAmperes);
                }
                if(row.length()>0) {
                    for(int i = lastMTU + 1; i < options.mtus; i++) {
                        row.append(",,,");
                    }
                    System.out.println(row);
                    row.delete(0,row.length());
                }
            }
        }
        finally {
            iter.close();
        }
    }
    
    public static void main(String[] args) throws IOException, DatabaseException {
        final Options options = new Options();
        if(!options.parseOptions(args)) return;
        boolean readOnly = !options.record;
        File dbFile = new File(options.dbFilename);
        dbFile.mkdirs();
        DatabaseManager databaseManager = new DatabaseManager(dbFile,readOnly,options);
        databaseManager.open();
        
        final Main main = new Main(options,databaseManager);

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                main.shutdown();
            }
        });

        try {
            if(options.serve) { 
                main.servlet = new Servlet(options,main.databaseManager);
                main.servlet.initMinAndMax();
                main.server = Servlet.setupServer(options,main.servlet);
                main.server.start();
            }

            if(options.deleteUntil > main.servlet.getMaximum()) {
                System.err.println("delete-until option would delete everything, ignoring");
            }
            else {
                // Always delete everything before 2009; got some due to bug in its-electric 1.4
                if(options.deleteUntil < 1230000000) options.deleteUntil = 1230000000;
                if(options.deleteUntil >= main.servlet.getMinimum()) {
                    main.servlet.setMinimum(databaseManager.secondsDb.minimumAfter(options.deleteUntil));
                    main.deleteTask = Executors.newSingleThreadExecutor();
                    for(final TimeSeriesDatabase db : databaseManager.databases) {
                        main.deleteTask.execute(db.new DeleteUntil(main, options.deleteUntil));
                    }                  
                    main.deleteTask.shutdown();
                }
            }

            if(options.record && options.ccPortName!=null) {
                main.catchUp = new CatchUp(main,options,main.databaseManager); 
                main.importer = new CurrentCostImporter(main, options, main.databaseManager, main.servlet, main.catchUp);
            }
            else if(options.record && (options.longImportInterval>0 || options.importInterval>0 || options.voltAmpereImportIntervalMS>0)) {
                main.catchUp = new CatchUp(main,options,main.databaseManager); 
                main.importer = new TedImporter(main, options, main.databaseManager, main.servlet, main.catchUp);
            }

            if(main.importer != null) {
                main.catchUpTask = Executors.newSingleThreadExecutor();
                main.catchUpTask.execute(main.catchUp);
                main.importer.startup();
            }
            
            if(options.export) {
                main.export();
            }
        }
        catch(Throwable e) {
            e.printStackTrace();
            main.shutdown();
        }
    }
}
