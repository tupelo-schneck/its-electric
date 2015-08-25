# Time Zone and DST Issues #

This document describes various issues around time zones and DST one may experience using its-electric, and workarounds.


## Time zone and DST issues with the TED 5000 Gateway ##

The TED 5000 Gateway does not emit universal timestamps of any sort.  It emits a local clock time.  In particular, the "fall back" change from DST to standard time causes there to be two distinct occurrences of every clock time from 01:00:00 to 01:59:59 on the day of change.

As of version 1.8, its-electric will make a best effort to determine which of the two times is intended.  (Previous versions always used the later of the two times, so the first hour was just missing.)

A foolproof way to get all the data is to tell the TED 5000 Gateway to not apply the DST change.  Then TED will go from 01:59:59 to 02:00:00 (instead of back to 01:00:00) and everything will work great.  You can instruct its-electric to still serve the data as if it were using DST using the `--ted-no-dst` option.

More generally, there are options `--record-time-zone` and `--serve-time-zone` to set the time zones used by the TED 5000 Gateway and to be used by the its-electric Google Visualization API datasource server; also `--time-zone` to set both.


## Time zone and DST issues with the Google Visualization Annotated Time Line ##

This section is more of technical interest and to record workarounds for the future.

The Annotated Time Line visualization used by the its-electric web pages to display the data also does not use universal timestamps; it expects to receive a local clock time.  The Java API used to send the data requires each time to be sent as if it were UTC, so the timestamps stored by its-electric are converted to UTC when sent.  This conversion is also done in the extra data sent in table properties.

Unfortunately, there is an [bug in the Annotated Time Line](http://groups.google.com/group/google-visualization-api/browse_thread/thread/cbe55119c22ac45a), known since at least April 2009, where the zoomStartTime and zoomEndTime options to the time line's draw() method are interpreted as UTC, rather than as a local clock time.  This means that when setting these options, the time needs to be converted from local clock time to the UTC time with the same wall clock (hh:mm:ss) value.  The code to do this is:

```
    // date a Date object (in local time zone), time in ms
    date.setTime(time);
    date.setTime(time - date.getTimezoneOffset()*60000);
    // yes, again, in case we are close to DST
    date.setTime(time - date.getTimezoneOffset()*60000);
```

Indeed, that last line is surprisingly required to get correct results when close to a DST changeover.