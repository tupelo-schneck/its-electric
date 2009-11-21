"it's electric": software for storing and viewing home energy monitoring data

Running "it's electric"
=======================

(1) You'll need to run the "it's electric" Java program, which polls 
the TED 5000 for data, stores it in a database along with averages 
over longer time spans, and runs the server which provides the data 
according to the Google Visualizations API.  The command is:
 
  java -jar its-electric-{version}.jar [options] database-directory

The required database-directory is the directory in your filesystem 
where the database will be stored.  Note: the database gets big, on 
the order of 2GB/month.

You also may need to specify the -m option to tell "it's electric" how 
many MTUs you have (if you have more than one), and the -g option to 
specify the location of the gateway.  (The default is http://TED5000 
but I don't know whether that works even on Windows.  If your TED 5000 
has IP address 192.168.1.99 and port 1234, use -g http://192.168.1.99:1234.)

The -p option allows you to specify the port where the server listens.
The default is 8081.

You can read about other options by using option "--help".

You can also pass options to java (before the -jar option).  I use
  -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog 
  -Dorg.apache.commons.logging.simplelog.defaultlog=trace
to have "it's electric" dump trace output.


(2) Next you'll need to set up the "it's electric" web files
its-electric.html and its-electric.js.  You may need to edit
its-electric.html and tell it how to contact the datasource server you
set up in part (1); it comes looking for "http://localhost:8081" which
may work fine for you, but will certainly need to be changed if you
want your data accessible over the Internet---it's a client-side
process to get the data and pass it to the chart.

Even if you're just looking at the data yourself, you may want to 
access the files through a webserver.  (On a Mac, this just means 
opening System Preferences to the Sharing pane, turning on Web 
Sharing, and putting the files in /Library/WebServer/Documents; then 
you can point your browser at http://localhost/its-electric.html.)  

If you do share your "it's electric" over the Web, make sure you 
include a link to the source download.  You can either make 
its-electric-{version}.zip available directly from your site, or 
change the link in its-electric.html to point back to where you 
downloaded it.  If you modify your source, you'll need to make your
modified copy available for download, as specified by the 
GNU Affero General Public License (see legal/COPYING-agpl.txt).

If you try to access its-electric.html using a file: URL, you may find 
that the automatic resolution-change on zoom doesn't work.  This has 
to do with Flash security settings.  You can fix this by going to the 
Flash Player Settings Manager and adding the relevant file to the 
trusted locations.  Go to

http://www.macromedia.com/support/documentation/en/flashplayer/help/settings_manager04.html  

click on "Add Location" in the "Edit Locations" dropdown, and browse to
its-electric.html.



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
likely to include them in future versions!



The Fine Print
==============

"it's electric": software for storing and viewing home energy monitoring data
Copyright (C) 2009 Robert R. Tupelo-Schneck <schneck@gmail.com>
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
