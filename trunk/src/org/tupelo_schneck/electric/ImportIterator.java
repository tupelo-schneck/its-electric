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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class ImportIterator implements Iterator<Triple> {
    static final DateFormat tedDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    InputStream urlStream;
    PushbackReader reader;
    byte mtu;
    volatile boolean closed;

    public ImportIterator(final String gatewayURL, final byte mtu, final int count) throws IOException {
        this.mtu = mtu;
        URL url;
        try {
            url = new URL(gatewayURL+"/history/secondhistory.xml?INDEX=0&MTU="+mtu+"&COUNT="+count);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(60000);
        urlConnection.setReadTimeout(60000);
        urlConnection.connect();
        urlStream = urlConnection.getInputStream();
        reader = new PushbackReader(new BufferedReader(new InputStreamReader(urlStream)),10);
        urlConnection.setReadTimeout(1000);
        skipAhead();
    }

    public void close() {
        closed = true;
        if(urlStream!=null) try { urlStream.close(); } catch (Throwable t) { t.printStackTrace(); }
        if(reader!=null) try { reader.close(); } catch (Throwable t) { t.printStackTrace(); }
    }

    boolean eof() throws IOException {
        int ch = reader.read();
        if(ch<0) return true;
        reader.unread(ch);
        return false;
    }

    boolean lookingAt(char ch) throws IOException {
        int next = reader.read();
        if(next<0) return false;
        reader.unread(next);
        return next == ch;
    }

    void skipWhitespace() throws IOException {
        while(true) {
            int ch = reader.read();
            if(ch<0) return;
            if(!Character.isWhitespace(ch)) {
                reader.unread(ch);
                return;
            }
        }
    }

    void skipString(String s) throws IOException {
        for(int i = 0; i < s.length(); i++) {
            if(lookingAt(s.charAt(i))) reader.read();
            else {
                for(int j = i-1; j >= 0; j--) {
                    reader.unread(s.charAt(j));
                }
                return;
            }
        }
    }

    void skipAhead() throws IOException {
        skipWhitespace();
        if(!lookingAt('<')) return;
        skipString("</POWER>");
        skipString("</DATE>");
        while(!eof() && !lookingAt('P') && !lookingAt('D')) {
            while(!eof() && !lookingAt('P') && !lookingAt('D')) {
                reader.read();
            }
            skipString("D>");
        }
        skipString("POWER>");
        skipString("DATE>");
    }

    String nextString() throws IOException {
        StringBuilder sb = new StringBuilder();
        while(!eof() && !lookingAt('<')) {
            sb.append((char)reader.read());
        }
        return sb.toString();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    @Override
    public boolean hasNext() {
        if(closed) return false;
        boolean res;
        try {
            skipAhead();
            res = !eof();
        }
        catch(SocketTimeoutException e) {
            res = false;
        }
        catch(IOException e) {
            e.printStackTrace();
            res = false;
        }
        if(!res) {
            close();
        }
        return res;
    }
    @Override
    public Triple next() {
        if(closed) return null;
        try {
            Triple res = new Triple();
            skipAhead();
            String dateString = nextString();
            skipAhead();
            res.power = Integer.valueOf(nextString());
            res.mtu = mtu;
            res.timestamp = (int)(ImportIterator.tedDateFormat.parse(dateString).getTime()/1000);
//            long diff = (System.currentTimeMillis() - 1000L * res.timestamp) / 1000;
//            // skip ridiculous values
//            if(Math.abs(diff) > 86400 || Math.abs(res.power) > 240 * 800) {
//                return next();
//            }
            return res;
        }
        catch(SocketTimeoutException e) {
            close();
            return null;
        }
        catch(Exception e) {
            close();
            throw new RuntimeException(e);
        }
    }
}