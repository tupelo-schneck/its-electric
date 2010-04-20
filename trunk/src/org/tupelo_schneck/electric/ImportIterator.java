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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

public class ImportIterator implements Iterator<Triple> {
    InputStream urlStream;
    GregorianCalendar cal = new GregorianCalendar();
    Base64 base64 = new Base64();
    byte mtu;
    byte[] line = new byte[25];
    volatile boolean closed;

    public ImportIterator(final String gatewayURL, final byte mtu, final int count) throws IOException {
        this.mtu = mtu;
        URL url;
        try {
            url = new URL(gatewayURL+"/history/rawsecondhistory.raw?INDEX=1&MTU="+mtu+"&COUNT="+count);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(60000);
        urlConnection.setReadTimeout(60000);
        urlConnection.connect();
        urlStream = urlConnection.getInputStream();
        urlConnection.setReadTimeout(1000);
        getNextLine();
    }

    public void close() {
        closed = true;
        if(urlStream!=null) try { urlStream.close(); } catch (Exception e) { e.printStackTrace(); }
    }

    public void getNextLine() {
        try {
            int n = 0;
            while(n < 25) {
                int r = urlStream.read(line,n,25-n);
                if(r <= 0) {
                    close();
                    return;
                }
                n += r;
            }
            if(line[24]!='\n') close();
        }
        catch(IOException e) {
            close();
        }
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean hasNext() {
        return !closed;
    }

    @Override
    public Triple next() {
        if(closed) return null;
        byte[] decoded = base64.decode(line);
        if(decoded==null || decoded.length<10) return null;
        cal.set(2000+(0x00FF & decoded[0]), decoded[1]-1, decoded[2], decoded[3], decoded[4], decoded[5]);
        int power = TimeSeriesDatabase.intOfBytes(decoded,6);
        Triple res = new Triple((int)(cal.getTimeInMillis() / 1000),mtu,power);
        getNextLine();
        return res;
   }
}