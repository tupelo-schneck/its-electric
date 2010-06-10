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

function ItsElectric(url,timelineId,busyId,resolutionId,initialZoom,realTimeUpdateInterval) {
    this.url = url;
    this.queryPath = "/power";
    this.timelineId = timelineId;
    this.initialZoom = initialZoom;
    this.busyId = busyId;
    this.resolutionId = resolutionId;

    this.div1 = null;
    this.div2 = null;

    this.ready = false;
    this.firstTime = true;
    this.range = null;
    this.resolution = null;
    this.resolutionString = "";
    this.minimum = 0;
    this.maximum = 0;

    this.partialRange = false;

    this.realTime = true; // set false to prevent auto-update at latest time

    this.noFlashEvents = false; // set true to make it work (somewhat) when
                                // accessing a file: URL without privileges

    var self = this;
    setInterval(function(){self.realTimeUpdate();},realTimeUpdateInterval);
}

ItsElectric.prototype.init = function() {
    var div0 = document.createElement('div');
    div0.style.position = 'relative';
    div0.style.width = '100%';
    div0.style.height = '100%';
    this.div1 = document.createElement('div');
    this.div1.style.position = 'absolute';
    this.div1.style.width = '100%';
    this.div1.style.height = '100%';
    this.div1.style.zIndex = '1';
    this.div1.style.visibility = 'visible';
    this.div2 = document.createElement('div');
    this.div2.style.position = 'absolute';
    this.div2.style.width = '100%';
    this.div2.style.height = '100%';
    this.div2.style.zIndex = '0';
    this.div2.style.visibility = 'hidden';
    document.getElementById(this.timelineId).appendChild(div0);
    div0.appendChild(this.div1);
    div0.appendChild(this.div2);

    this.annotatedtimeline = new google.visualization.AnnotatedTimeLine(this.div1);
    this.annotatedtimeline2 = new google.visualization.AnnotatedTimeLine(this.div2);

    var self = this;
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'ready',
                                            function(e){self.readyHandler(e);});
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e);});
    google.visualization.events.addListener(this.annotatedtimeline2,
                                            'ready',
                                            function(e){self.readyHandler(e);});
    google.visualization.events.addListener(this.annotatedtimeline2,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e);});
    this.requery();
};

ItsElectric.prototype.queryURL = function() {
    var realTimeNeedsAdjust = this.realTime && this.range && this.range.end.getTime() == this.maximum;
    var queryURL = this.url;
    if(queryURL.charAt(queryURL.length-1)=='/' && this.queryPath.charAt(0)=='/') {
      queryURL = queryURL + this.queryPath.substring(1);
    }
    else {
      queryURL = queryURL + this.queryPath;
    } 
    var extendChar = '?';
    if(true) { 
        queryURL = queryURL + extendChar + 'extraPoints=2';
        extendChar = '&';
    }
    if(realTimeNeedsAdjust) {
        queryURL = queryURL + extendChar + 'realTimeAdjust=yes';
        extendChar = '&';
    }
    if(this.ready) {
        if(this.range && (this.range.start.getTime() != this.minimum || this.range.end.getTime() != this.maximum)) {
            var start = Math.floor(this.range.start.getTime()/1000);
            var end = Math.floor(this.range.end.getTime()/1000);
            queryURL = queryURL + extendChar +
                       'start='+ start +
                       '&end=' + end;
            extendChar = '&';
            if(this.partialRange) {
                var rangeStart = start - (end - start);
                rangeStart = Math.floor(Math.max(this.minimum/1000, rangeStart));
                var rangeEnd = end + (end - start);
                rangeEnd = Math.floor(Math.max(this.minimum/1000, rangeStart));
                queryURL = queryURL + extendChar +
                        'rangeStart=' + rangeStart +
                        '&rangeEnd=' + rangeEnd;
                extendChar = '&';
            }
        }
    }
    if(this.resolution) {
        queryURL = queryURL + extendChar +
                   'resolution=' + this.resolution;
        extendChar = '&';
    }
    return queryURL;
}

ItsElectric.prototype.requery = function() {
    var query = new google.visualization.Query(this.queryURL());
    if(this.busyId) document.getElementById(this.busyId).style.display="";
    var self = this;
    query.send(function(response) {self.handleQueryResponse(response);});
};

ItsElectric.prototype.options = {displayAnnotations: false, 
                                 displayExactValues: true,
                                 allValuesSuffix: 'W',
                                 dateFormat: 'yyyy-MM-dd HH:mm:ss'};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        if(this.busyId) document.getElementById(this.busyId).style.display="none";
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var data = response.getDataTable();
    
    var realTimeNeedsAdjust = this.realTime && this.range && this.range.end.getTime() == this.maximum;

    var rangeStart = data.getValue(0,0).getTime();
    this.minimum = parseInt(data.getTableProperty('minimum'))*1000;
    if(isNaN(this.minimum) || this.minimum==0) {
        this.minimum = rangeStart;
    }
    var rangeEnd = data.getValue(data.getNumberOfRows()-1,0).getTime()
    this.maximum = parseInt(data.getTableProperty('maximum'))*1000;
    if(isNaN(this.maximum) || this.maximum==0) {
        this.maximum = rangeEnd;
    }

    if(this.partialRange) {
        if(this.minimum < this.rangeStart) {
            data.insertRows(0,1);
            data.setValue(0,0,new Date(this.minimum));
            for(var i = 1; i < data.getNumberOfColumns(); i++) {
                data.setValue(0,i,null);
            }
        }
        if(this.maximum > this.rangeEnd) {
            var newRow = data.addRow();
            data.setValue(newRow,0,new Date(this.maximum));
            for(var i = 1; i < data.getNumberOfColumns(); i++) {
                data.setValue(0,i,null);
            }
        }
    }

    this.timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
    
    var options = {};
    for(p in this.options) options[p] = this.options[p];

    var startDate = new Date();
    var endDate = new Date();
    var start;
    var end;
    if(this.range) {
        if(realTimeNeedsAdjust) {
            start = this.maximum - (this.range.end.getTime() - this.range.start.getTime());
            end = this.maximum;
            this.range.start.setTime(start);
            this.range.end.setTime(end);
        }
        else {
            start = this.range.start.getTime();
            end = this.range.end.getTime();
        }
    }
    else {
        start = this.minimum;
        end = this.maximum;
    }
    setDateAdjusted(startDate,start);
    setDateAdjusted(endDate,end);
    options.zoomStartTime = startDate;
    options.zoomEndTime = endDate;
    this.resolutionString = data.getTableProperty('resolutionString');
    
    this.data = data;
    this.redraw();
};

ItsElectric.prototype.redraw = function() {
    if(this.data==null) return;
    if(this.busyId) document.getElementById(this.busyId).style.display="";

    this.annotatedtimeline2.draw(data, options);

    if(this.noFlashEvents) this.readyHandler(null);
};

// this horribleness makes things behave close to daylight saving time change
function setDateAdjusted(date,time) {
    date.setTime(time);
    date.setTime(time - date.getTimezoneOffset()*60000);
    // yes, again, in case we are close to DST
    date.setTime(time - date.getTimezoneOffset()*60000);
}

ItsElectric.prototype.readyHandler = function(e) {
    if(this.busyId) document.getElementById(this.busyId).style.display="none";
    if(this.resolutionId) {
        var obj = document.getElementById(this.resolutionId);
        while(obj.firstChild) obj.removeChild(obj.firstChild);
        obj.appendChild(document.createTextNode(this.resolutionString));
    }

    var temp = this.annotatedtimeline2;
    this.annotatedtimeline2 = this.annotatedtimeline;
    this.annotatedtimeline = temp;
    temp = this.div1.style.zIndex;
    this.div1.style.zIndex = this.div2.style.zIndex;
    this.div2.style.zIndex = temp;
    temp = this.div1.style.visibility;
    this.div1.style.visibility = this.div2.style.visibility;
    this.div2.style.visibility = temp;

    this.ready = true;
    if(this.firstTime && !this.noFlashEvents) {
        this.firstTime = false;
        this.zoom(this.initialZoom);
    }
};

ItsElectric.prototype.rangeChangeHandler = function(e) {
    var oldRange = 0;
    if(this.range) {
        oldRange = this.range.end.getTime() - this.range.start.getTime();
    }
    this.range = this.annotatedtimeline.getVisibleChartRange();
    if(this.range.end.getTime() - this.range.start.getTime() != oldRange) {
        this.resolution = null;
    }
    this.requery();
};

ItsElectric.prototype.zoom = function(t) {
    if(this.noFlashEvents) alert("This won't work with noFlashEvents=true.");
    if(!this.ready || this.noFlashEvents) return;
    this.range = this.annotatedtimeline.getVisibleChartRange();
    this.resolution = null;
    var newStart = new Date();
    var newEnd = new Date();
    if(t==null) {
        newStart.setTime(this.minimum);
        this.range.start.setTime(this.minimum);
        newEnd.setTime(this.maximum);
        this.range.end.setTime(this.maximum);        
    }
    else {
        newStart.setTime(this.range.end.getTime() - t*1000);
        if(newStart.getTime()<this.minimum) newStart.setTime(this.minimum);
        this.range.start.setTime(newStart.getTime());
        newEnd.setTime(this.range.end.getTime());
    }
    this.annotatedtimeline.setVisibleChartRange(newStart,newEnd);
    var self = this;
    setTimeout(function(){self.requery();},500);
};

ItsElectric.prototype.scrollToPresent = function() {
    if(this.noFlashEvents) alert("This won't work with noFlashEvents=true.");
    if(!this.ready || this.noFlashEvents) return;
    this.range = this.annotatedtimeline.getVisibleChartRange();
    var size = this.range.end.getTime() - this.range.start.getTime();
    var newStart = new Date();
    var newEnd = new Date();
    newEnd.setTime(this.maximum);
    this.range.end.setTime(this.maximum);
    newStart.setTime(this.maximum - size);
    this.range.start.setTime(this.maximum - size);
    this.annotatedtimeline.setVisibleChartRange(newStart,newEnd);
    var self = this;
    setTimeout(function(){self.requery();},500);
};

ItsElectric.prototype.setResolution = function(t) {
    if(!this.ready) return;
    this.resolution = t;
    this.requery();
};

ItsElectric.prototype.realTimeUpdate = function() {
    if(!this.ready || !this.realTime || this.noFlashEvents) return;
    if(this.range && this.range.end.getTime() < this.maximum) return;
    this.requery();
};
