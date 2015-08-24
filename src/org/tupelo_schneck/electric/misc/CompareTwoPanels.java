/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2010--2015 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
package org.tupelo_schneck.electric.misc;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.tupelo_schneck.electric.DatabaseManager;
import org.tupelo_schneck.electric.Main;
import org.tupelo_schneck.electric.Options;
import org.tupelo_schneck.electric.Triple;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;
import org.tupelo_schneck.electric.Util;

import com.ibm.icu.util.GregorianCalendar;
import com.sleepycat.je.DatabaseException;

/**
 * This is an example of writing a separate program to use the its-electric database.
 */
public class CompareTwoPanels {

    public static void main(String[] args) throws IOException, DatabaseException {
        final Options options = new Options();
        if(!options.parseOptions(args)) return;
        boolean readOnly = true;
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
            System.out.println("Doing it:");
            
            double[] total = new double[4];
            double[] count = new double[4];
            
            ReadIterator iter = databaseManager.secondsDb.read((int)(new GregorianCalendar(2011,6-1,18,0,0,0).getTimeInMillis()/1000),
                    (int)(new GregorianCalendar(2011,8-1,1,0,0,0).getTimeInMillis()/1000));
            try {
                while(iter.hasNext()) {
                    Triple t = iter.next();
                    if(t.power==null) continue;
                   // if(t.mtu < 2) continue;
                    int power = t.power.intValue();
                    if(t.timestamp % 86400 == 0) System.out.println(Util.dateString(t.timestamp));
                    total[t.mtu]+=power;
                    count[t.mtu]++;
                }
            }
            finally {
                iter.close();
            }
            
            PrintStream out = System.out;
            out.println("MTU1: " + total[0]/count[0]);
            out.println("MTU2: " + total[1]/count[1]);
            out.println("MTU3: " + total[2]/count[2]);
            out.println("MTU4: " + total[3]/count[3]);
            out.println();
        }
        catch(Throwable e) {
            e.printStackTrace();
            main.shutdown();
        }
    }

}
