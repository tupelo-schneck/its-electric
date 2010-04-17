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

import java.util.Comparator;

public class Triple {
    public final int timestamp;
    public final byte mtu;
    public final int power;
    
    public Triple(int timestamp, byte mtu, int power) {
        this.timestamp = timestamp;
        this.mtu = mtu;
        this.power = power;
    }

    public static final Comparator<Triple> COMPARATOR = new Comparator<Triple>() {
        @Override
        public int compare(Triple o1, Triple o2) {
            return o1.timestamp - o2.timestamp;
        }
    };
}