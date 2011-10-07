package org.tupelo_schneck.electric;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

public class VoltAmpereFetcher {
    String gatewayURL;
    Scanner scanner;
    byte mtus;
    TimeZone timeZone;
    
    static Pattern skipPattern = Pattern.compile("(?>" + "<[^PG][^>]*+>" + "|" + "<P[^>]{5,}+>" + "|" + "[^<]++" + ")*+");
    static Pattern skipToMTUPattern = Pattern.compile("(?>" + "<[^M][^>]*+>" + "|" + "<M[^T][^>]*+>" + "|" + "[^<]++" + ")*+");
    static Pattern gatewayTimeChunkPattern = Pattern.compile("<GatewayTime>[^G]*+GatewayTime>");
    static Pattern mtuChunkPattern = Pattern.compile("<MTU[1-4]>[^U]*+U[1-4]>");
    static Pattern kvaPattern = Pattern.compile("<KVA>([^<]*+)</KVA>");
    static Pattern mtuNumberPattern = Pattern.compile("^<MTU([1-4])>");
    static Pattern yearPattern =  Pattern.compile("<Year>([^<]*+)</Year>");
    static Pattern monthPattern =  Pattern.compile("<Month>([^<]*+)</Month>");
    static Pattern dayPattern =  Pattern.compile("<Day>([^<]*+)</Day>");
    static Pattern hourPattern =  Pattern.compile("<Hour>([^<]*+)</Hour>");
    static Pattern minutePattern =  Pattern.compile("<Minute>([^<]*+)</Minute>");
    static Pattern secondPattern =  Pattern.compile("<Second>([^<]*+)</Second>");
    
    public VoltAmpereFetcher(Options options) {
        this.gatewayURL = options.gatewayURL;
        this.mtus = options.mtus;
        this.timeZone = options.recordTimeZone;
    }
    
    private void connect() throws IOException {
        URL url;
        try {
            url = new URL(gatewayURL+"/api/LiveData.xml");
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(60000);
        urlConnection.setReadTimeout(60000);
        urlConnection.connect();
        InputStream urlStream = urlConnection.getInputStream();
        urlConnection.setReadTimeout(1000);
        this.scanner = new Scanner(new BufferedReader(new InputStreamReader(urlStream)));
    }

    /** Returns 0 if failure */
    private int gatewayTime() {
        this.scanner.skip(skipPattern);
        String gatewayTimeChunk = this.scanner.findWithinHorizon(gatewayTimeChunkPattern, 4096);
        if(gatewayTimeChunk==null) return 0;
        int year,month,day,hour,minute,second;
        try {
            Matcher m = yearPattern.matcher(gatewayTimeChunk);
            if(!m.find()) return 0;
            year = Integer.parseInt(m.group(1));
            m.usePattern(monthPattern);
            if(!m.find(0)) return 0;
            month = Integer.parseInt(m.group(1));
            m.usePattern(dayPattern);
            if(!m.find(0)) return 0;
            day = Integer.parseInt(m.group(1));
            m.usePattern(hourPattern);
            if(!m.find(0)) return 0;
            hour = Integer.parseInt(m.group(1));
            m.usePattern(minutePattern);
            if(!m.find(0)) return 0;
            minute = Integer.parseInt(m.group(1));
            m.usePattern(secondPattern);
            if(m.find(0)) {
                second = Integer.parseInt(m.group(1));
            }
            else {
                second = new GregorianCalendar().get(GregorianCalendar.SECOND);
            }
        }
        catch(NumberFormatException e) {
            return 0;
        }
            
        GregorianCalendar cal = new GregorianCalendar(timeZone);
        cal.set(2000+year,month-1,day,hour,minute,second);
        int timestamp = (int)(cal.getTimeInMillis() / 1000);
        if(Util.inDSTOverlap(timeZone, timestamp)) {
            int now = (int)(System.currentTimeMillis()/1000);
            if(now < timestamp - 1800) timestamp -= 3600;
        }
        return timestamp;
    }
    
    private Triple nextKVA(int timestamp) {
        String mtuChunk = this.scanner.findWithinHorizon(mtuChunkPattern, 4096);
        if(mtuChunk==null) return null;
        try {
            Matcher m = mtuNumberPattern.matcher(mtuChunk);
            if(!m.find()) return null;
            byte mtu = Byte.parseByte(m.group(1));
            m.usePattern(kvaPattern);
            if(!m.find()) return null;
            int kva = Integer.parseInt(m.group(1));
            
            return new Triple(timestamp,(byte)(mtu-1),null,null,Integer.valueOf(kva));
        }
        catch(NumberFormatException e) {
            return null;
        }
    }
    
    public List<Triple> doImport() {
        List<Triple> res = new ArrayList<Triple>();
        try {
            connect();
        }
        catch(IOException e) {
            return res;
        }
        try {
            int timestamp = gatewayTime();
            if(timestamp==0) return res;
            
            this.scanner.skip(skipPattern);
            this.scanner.skip(skipToMTUPattern);
            
            for(int i = 0; i < mtus; i++) {
                Triple triple = nextKVA(timestamp);
                if(triple==null || triple.mtu >= mtus) return res;
                res.add(triple);
            }
            
            return res;
        }
        finally {
            this.scanner.close();
        }
    }
}
