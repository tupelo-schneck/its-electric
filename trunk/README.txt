"it's electric": software for storing and viewing home energy monitoring data

"it's electric": what it does
=============================

"it's electric" has two components: (1) a Java program and (2) a collection of
web pages with HTML, CSS, and Javascript.

The "it's electric" Java program is designed to perform two simultaneous
activities:
(1) it records data from TED (or other device) into a permanent database; and
(2) it serves data from the database in Google Visualization API format.

"it's electric" works with TED 5000 (heavily tested), Ted Pro (somewhat
tested), and Current Cost (barely tested).

The "it's electric" web pages will then talk to the server set up by the Java
program in order to display the data as an interactive chart.  You can simply
view the web pages yourself as files on your computer, or serve them up using 
a Web server for yourself or the general public to view over the network.

Note that you can also design your own web pages using the Google Visualization
API which talk to your "it's electric" datasource server.

For those not interested in the interactive charts, the "it's electric" Java
program can also export data from the "it's electric" database in CSV format, 
which you can then import into other databases or spreadsheets for analysis.


Quick Start Guide
=================

0) Install Java (Mac: automatic; Windows: visit java.com) and Adobe
Flash Player ( http://get.adobe.com/flashplayer/ )

1) Extract all files from its-electric-1.10.zip.

2) Open the command prompt and cd to the extracted its-electric directory.

3) Type the command:
java -jar its-electric-1.10.jar -g http://192.168.1.99 -m 2 -d its-electric-db
BUT CHANGE "192.168.1.99" to the IP address of your TED Gateway, and
change "2" (in "-m 2") to the number of MTUs in your TED system.

(Note: if your TED Gateway is automatically assigned an IP address, you can
try using -g http://TED5000 which is known to work on Windows.)

If using TED Pro use arguments like: --device ted-pro --spyders 16

If using Current Cost try "java -jar its-electric-1.10.jar -h" to list 
available options.

4) Tell Flash to trust the its-electric web pages by going to the Flash Player 
Settings Manager and adding that file to the trusted locations.  Go to

http://www.macromedia.com/support/documentation/en/flashplayer/help/settings_manager04.html

click on "Add Location" in the "Edit Locations" dropdown, and 
"Browse for folder" to the extracted its-electric/web subdirectory.

5) Open its-electric/web/its-electric.html in your browser (for instance, 
by double-clicking it). 

6) (optional) If you want to monitor voltage, add "-v" to the command in 
step (3).  If you want to monitor kVA, add "-k 2" where 2 is the number of 
seconds "it's electric" waits between polls for kVA data.  You'll also need 
to edit "its-electric-config.js" and change "hasVoltage" and/or "hasKVA" to 
"true".


Installation advice from users
==============================

I've highlighted a couple of posts by users, detailing installation on their
platforms, at 
http://code.google.com/p/its-electric/wiki/InstallationAdviceFromUsers



Running "it's electric": in detail
==================================

(1) You'll need to run the "it's electric" Java program, which polls 
the TED 5000 (or TED Pro, or Current Cost) for data, stores it in a database 
along with averages over longer time spans, and runs the server which provides 
the data according to the Google Visualizations API.  The command is:
 
  java -jar its-electric-{version}.jar [options]

The option --device specifies the device.  Default is ted-5000, other 
options are ted-pro and current-cost.  Use option -h/--help to list 
all available options.

The option -d is required, to specify the directory in your filesystem 
where the database will be stored.  Note: the database gets big, on 
the order of 1GB/month.  (More precisely, around 250MB/month/MTU.)

You also may need to specify the -m option to tell "it's electric" how 
many MTUs you have (if you have more than one).

For recording data, you may need the -g option to specify the location of the 
gateway.  (The default is http://TED5000.  If your TED 5000 has IP address 
192.168.1.99 and port 1234, use -g http://192.168.1.99:1234.)  If you include 
option "-v", then its-electric will monitor the voltage data from TED as well 
as power.  If you include "-k 2", its-electric will poll for kVA data every 
2 seconds (unlike voltage and real power, kVA data is not included in the TED 
history data).

For serving data, the -p option allows you to specify the port where the server 
listens.  The default is 8081.  Note: -g specifies the URL (generally with IP 
address and port) where the its-electric server will contact TED; -p specifies 
the port where the its-electric web pages will contact the its-electric server.

You can specify to only record data using option --no-serve (e.g. for an
unattended setup) and to only serve data using option --no-record (e.g. with a
static copy of an its-electric database).

To export data from <start> to <end> in resolution <res>, use
  java -jar its-electric-*.jar -d <database-dir> --export <start> <end> <res>
<start> and <end> are expected in ISO 8601 format, for example 
  2010-11-05T22:09:37-0400
You can leave off components and "it's electric" should behave sensibly, e.g.
  java -jar its-electric-*.jar -d <database-dir> --export 2010-08 2010-10 1
will export data for all seconds in August, September, and October.

You can read about other options by using option "--help".

You can also pass options to java itself (before the -jar option).  I use
  -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog 
  -Dorg.apache.commons.logging.simplelog.defaultlog=trace
to have "it's electric" dump trace output.

If you have memory issues, consider giving java more memory using
an option like -server or -Xmx128M .  In fact consider that in any case;
I particularly recommend java -server for this.


(2) Next you'll need to set up the "it's electric" web files, in the 
extracted its-electric/web directory.  You may need to edit the 
file its-electric-config.js file to tell how to contact the datasource 
server you set up in part (1); it comes looking for "http://localhost:8081" 
which may work fine for you, but will certainly need to be changed if you 
want your data accessible over the Internet---it's a client-side process 
to get the data and pass it to the chart.  Change the property
"datasourceURL".  If you're storing voltage and/or kVA data, change 
"hasVoltage" and/or "hasKVA" to "true".

Even if you're just looking at the data yourself, you may want to 
access the files through a webserver.  (On a Mac, this just means 
opening System Preferences to the Sharing pane, turning on Web 
Sharing, and putting the files in /Library/WebServer/Documents; then 
you can point your browser at http://localhost/its-electric.html.)  

***IMPORTANT***:
If you try to access its-electric.html using a file: URL, it's 
probably not going to work.  This has to do with Flash security 
settings.  You can fix this by going to the Flash Player Settings 
Manager and adding the relevant files to the trusted locations.  
Go to

http://www.macromedia.com/support/documentation/en/flashplayer/help/settings_manager04.html  

click on "Add Location" in the "Edit Locations" dropdown, and browse to
the folder "its-electric/web".

(If you really want to use a file: URL without doing that, you can try
setting "noFlashEvents: true" in its-electric-config.js, and it should limp 
along.  Not recommended.)

If you do share your "it's electric" over the Web, make sure you include 
a link to the source download.  If you modify your source, you'll need 
to make your modified copy available for download, as specified by the 
GNU Affero General Public License (see legal/COPYING-agpl.txt).


The Discussion Group
====================

Join the discussion group at 
http://groups.google.com/group/its-electric-software
and share your experiences, feature requests, bug reports, etc.!


The Source
==========

The full source of "it's electric" is included in the 
its-electric-{version}.jar file.  If you unzip it, you'll have a copy
of the Eclipse project that I used to develop it, and an Ant build file
to recreate it.  Please tell me about your modifications, I'm quite
likely to include them in future versions!  We also have a Google Code
project at 
http://code.google.com/p/its-electric/


The Fine Print
==============

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

"it's electric" includes third-party code, the various copyright notices
and licenses for which may be found in the files in legal/third-party
included in the "it's electric" distribution.
