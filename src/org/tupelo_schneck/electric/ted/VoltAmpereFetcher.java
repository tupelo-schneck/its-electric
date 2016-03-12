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

package org.tupelo_schneck.electric.ted;

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

import org.tupelo_schneck.electric.Options;
import org.tupelo_schneck.electric.Triple;
import org.tupelo_schneck.electric.Util;

import com.ibm.icu.util.GregorianCalendar;
import com.ibm.icu.util.TimeZone;

public class VoltAmpereFetcher {
    String gatewayURL;
    Scanner scanner;
    byte mtus;
    TimeZone timeZone;
    final Options options;
    
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
    
    static Pattern tedProGatewayTimePattern = Pattern.compile("<Time>([0-9]*+)</Time>");
    static Pattern tedProMtuPattern = Pattern.compile("<MTU([0-9]*)>");

    public VoltAmpereFetcher(Options options) {
        this.gatewayURL = options.gatewayURL;
        this.mtus = options.mtus;
        this.timeZone = options.recordTimeZone;
        this.options = options;
    }
    
    private String getUrl() {
        if ("ted-pro".equals(options.device)) {
            return gatewayURL + "/api/SystemOverview.xml?T=0";
        } else {
            return gatewayURL+"/api/LiveData.xml";
        }
    }
    
    private void connect() throws IOException {
        URL url;
        try {
            url = new URL(getUrl());
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
    private int gatewayTime() throws IOException {
        if ("ted-pro".equals(options.device)) {
            return tedProGatewayTime();
        } else {
            return ted5000GatewayTime();
        }
    }

    private int tedProGatewayTime() throws IOException {
        URL url;
        try {
            url = new URL(gatewayURL + "/api/Rate.xml");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(60000);
        urlConnection.setReadTimeout(60000);
        urlConnection.connect();
        InputStream urlStream = urlConnection.getInputStream();
        Scanner timeScanner = new Scanner(urlStream);
        try {
            urlConnection.setReadTimeout(1000);
            String gatewayTimeChunk = timeScanner.findWithinHorizon(tedProGatewayTimePattern, 4096);
            if (gatewayTimeChunk == null) return 0;
            Matcher m = tedProGatewayTimePattern.matcher(gatewayTimeChunk);
            if (!m.matches()) return 0;
            return Integer.parseInt(m.group(1));
        } catch (NumberFormatException e) {
            return 0;
        } finally {
            timeScanner.close();
        }
    }
    
    private int ted5000GatewayTime() {
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

        this.scanner.skip(skipPattern);
        this.scanner.skip(skipToMTUPattern);
        
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
        if ("ted-pro".equals(options.device)) {
            return tedProNextKVA(timestamp);
        } else {
            return ted5000NextKVA(timestamp);
        }
    }
    
    private Triple ted5000NextKVA(int timestamp) {
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
    
    private Triple tedProNextKVA(int timestamp) {
        String mtuChunk = this.scanner.findWithinHorizon(tedProMtuPattern, 4096);
        if(mtuChunk==null) return null;
        try {
            Matcher m = tedProMtuPattern.matcher(mtuChunk);
            if(!m.matches()) return null;
            byte mtu = Byte.parseByte(m.group(1));
            String kvaChunk = this.scanner.findWithinHorizon(kvaPattern, 4096);
            m = kvaPattern.matcher(kvaChunk);
            if(!m.matches()) return null;
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
            
            for(int i = 0; i < mtus; i++) {
                Triple triple = nextKVA(timestamp);
                if(triple==null || triple.mtu >= mtus) return res;
                res.add(triple);
            }
            
            return res;
        }
        catch(IOException e) {
            return res;
        }
        finally {
            this.scanner.close();
        }
    }
}
