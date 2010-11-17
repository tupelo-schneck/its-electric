/*
This file is part of
"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2010 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
import java.io.PrintStream;

import org.tupelo_schneck.electric.Main;
import org.tupelo_schneck.electric.Triple;
import org.tupelo_schneck.electric.TimeSeriesDatabase.ReadIterator;

import com.ibm.icu.util.GregorianCalendar;

/**
 * I wrote this utility for when I had 2 MTUs hooked up to 2 water heaters.
 * It measures the total use, and attempts to measure standby loss 
 * by creating a histogram of how much energy is used in each cycle (presumably 
 * a standby-loss-only cycle is the lowest commonly observed cycle).
 * 
 * This is an example of writing a separate program to use the its-electric database.
 */
public class WaterHeaterReader {

    public static void main(String[] args) {
        final Main main = new Main();
        main.readOnly = true;
        
        try {
            if(!main.options.parseOptions(args)) return;
            File dbFile = new File(main.options.dbFilename);
            dbFile.mkdirs();
            main.openEnvironment(dbFile);
            main.openDatabases();

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    main.shutdown();
                }
            });

            System.out.println("Doing it:");
            
            double[] total = new double[2];
            double[] count = new double[2];
            int max = 0;
            
            boolean[] on = new boolean[2];
            double[] usedSinceLastOff = new double[2];
            double[] countSinceLastOff = new double[2];
            int[][] histo = new int[2][20];
            
            ReadIterator iter = main.secondsDb.read((int)(new GregorianCalendar(2010,10-1,1,0,0,0).getTimeInMillis()/1000),
                    (int)(new GregorianCalendar(2010,11-1,1,0,0,0).getTimeInMillis()/1000));
            try {
                while(iter.hasNext()) {
                    Triple t = iter.next();
                    if(t.power==null) continue;
                    int power = t.power.intValue();
                    if(t.timestamp % 86400 == 0) System.out.println(Main.dateString(t.timestamp));
                    total[t.mtu]+=power;
                    count[t.mtu]++;
                    if(power > max) max = power;
                    
                    if(on[t.mtu] && power < 20) {
                        double av = usedSinceLastOff[t.mtu]/countSinceLastOff[t.mtu];
                        int bucket = (int)(av / 10.0);
                        if(bucket>=20) bucket = 19;
                        histo[t.mtu][bucket]++;
                        usedSinceLastOff[t.mtu] = 0;
                        countSinceLastOff[t.mtu] = 0;
                    }
                    
                    on[t.mtu] = power >= 20;
                    usedSinceLastOff[t.mtu] += power;
                    countSinceLastOff[t.mtu] ++;
                }
            }
            finally {
                iter.close();
            }
            
            PrintStream out = System.out;
            out.println("Max: " + max);
            out.println("Av1: " + total[0]/count[0]);
            out.println("Av2: " + total[1]/count[1]);
            out.println();
            for(int i = 0; i < 20; i++) {
                out.println("Cycles(0," + i + "): " + histo[0][i]);
            }
            out.println();
            for(int i = 0; i < 20; i++) {
                out.println("Cycles(1," + i + "): " + histo[1][i]);
            }
        
        }
        catch(Throwable e) {
            e.printStackTrace();
            main.shutdown();
        }
    }

}
