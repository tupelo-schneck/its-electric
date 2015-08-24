/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009--2015 Robert R. Tupelo-Schneck <schneck@gmail.com>
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DatabaseManager {
    private Log log = LogFactory.getLog(DatabaseManager.class);

    public static final String[] durationStrings = new String[] {
        "1\"","4\"","15\"","1'","4'","15'","1h","3h","8h","1d"
    };
    public static final int[] durations = new int[] { 
        1, 4, 15, 60, 60*4, 60*15, 60*60, 60*60*3, 60*60*8, 60*60*24
    };
    public static final int numDurations = durations.length;

    public Environment environment;
    public final TimeSeriesDatabase[] databases = new TimeSeriesDatabase[numDurations];
    public TimeSeriesDatabase secondsDb;

    private boolean closed;
    
    private final File envHome;
    private final boolean readOnly;
    private final Options options;
    
    public DatabaseManager(File envHome, boolean readOnly, Options options) {
        this.envHome = envHome;
        this.readOnly = readOnly;
        this.options = options;
    }
    
    public void open() throws DatabaseException {
        EnvironmentConfig configuration = new EnvironmentConfig();
        configuration.setTransactional(false);
        // this seems to help with memory issues
        configuration.setCachePercent(40);
        long maxMem = Runtime.getRuntime().maxMemory();
        if(maxMem/100*(100-configuration.getCachePercent()) < 60L * 1024 * 1024) {
            configuration.setCacheSize(maxMem - 60L * 1024 * 1024);
            log.info("Cache size set to: " + (maxMem - 60L * 1024 * 1024));
        }
        configuration.setAllowCreate(true);
        configuration.setReadOnly(readOnly);
        environment = new Environment(envHome, configuration);
        log.info("Environment opened.");

        for(int i = 0; i < numDurations; i++) {
            databases[i] = new TimeSeriesDatabase(environment, readOnly, String.valueOf(durations[i]), options.mtus, (byte)(options.mtus + options.spyders), durations[i], durationStrings[i], options.serveTimeZone.getRawOffset() / 1000);
            log.trace("Database " + i + " opened");
        }
        secondsDb = databases[0];
    }
    
    public synchronized void close() {
        if(closed) return;
        for(TimeSeriesDatabase db : databases) {
            if(db!=null) db.close();
        }
        if(environment!=null) try { 
            environment.close(); 
            log.info("Environment closed.");
        } catch (Exception e) { e.printStackTrace(); }
        closed = true;
    }


}
