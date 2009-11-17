package org.tupelo_schneck.electric;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ImportIterator implements Iterator<Triple> {
    static final DateFormat tedDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    // lazy coder's non-blocking IO.  I was finding that the importer would sometimes hang 
    // on read()---the TED5000 was neither closing the connection nor sending more bytes,
    // indefinitely?  So I put time limits on all IO using java.concurrent stuff.
    ExecutorService executor = Executors.newSingleThreadExecutor();

    InputStream urlStream;
    PushbackReader reader;
    byte mtu;
    volatile boolean closed;

    public ImportIterator(final String gatewayURL, final byte mtu, final int count) throws Exception {
        this.mtu = mtu;
        Future<?> future = executor.submit(new Callable<Object>(){
            @Override
            public Object call() throws Exception {
                URL url;
                try {
                    url = new URL(gatewayURL+"/history/secondhistory.xml?INDEX=0&MTU="+mtu+"&COUNT="+count);
                }
                catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                urlStream = url.openStream();
                reader = new PushbackReader(new BufferedReader(new InputStreamReader(urlStream)),10);
                skipAhead();
                return null;
            }
        });
        try {
            future.get(60,TimeUnit.SECONDS);
        }
        catch(TimeoutException e) {
            future.cancel(true);
            close();
        }

    }

    public void close() {
        closed = true;
        new Thread() {
            public void run() {
                // things go awry if we just close the reader when it is blocked...
                if(urlStream!=null) try { urlStream.close(); } catch (Throwable t) { t.printStackTrace(); }
                if(reader!=null) try { reader.close(); } catch (Throwable t) { t.printStackTrace(); }
            }
        }.start();
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
            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    skipAhead();
                    return !eof();
                }
            });
            try {
                res = future.get(500,TimeUnit.MILLISECONDS);
            }
            catch(TimeoutException e) {
                future.cancel(true);
                res = false;
            }
        }
        catch(Exception e) {
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
        final Triple res = new Triple();
        try {
            Future<?> future = executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    skipAhead();
                    String dateString = nextString();
                    skipAhead();
                    res.power = Integer.valueOf(nextString());
                    res.mtu = mtu;
                    res.timestamp = (int)(ImportIterator.tedDateFormat.parse(dateString).getTime()/1000);
                    return null;
                } 
            });
            try {
                future.get(500,TimeUnit.MILLISECONDS);
            }
            catch(TimeoutException e) {
                future.cancel(true);
                close();
                return null;
            }
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }

        return res;
    }
}