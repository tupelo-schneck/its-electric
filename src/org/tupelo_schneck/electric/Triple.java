package org.tupelo_schneck.electric;

import java.util.Comparator;

public class Triple {
    public int timestamp;
    public byte mtu;
    public int power;
    
    public static final Comparator<Triple> COMPARATOR = new Comparator<Triple>() {
        @Override
        public int compare(Triple o1, Triple o2) {
            return o1.timestamp - o2.timestamp;
        }
    };
}