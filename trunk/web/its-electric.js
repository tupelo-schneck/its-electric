/*
This file is part of
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
*/

function ItsElectric(url,timelineId,busyId,getWMax,resolutionId) {
    this.url = url;
    this.timelineId = timelineId;
    this.busyId = busyId;
    this.getWMax = getWMax;
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

    this.realTime = true; // set false to prevent auto-update at latest time

    this.noFlashEvents = false; // set true to make it work (somewhat) when
                                // accessing a file: URL without privileges

    var self = this;
    setInterval(function(){self.realTimeUpdate();},60000);
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
    this.div2 = document.createElement('div');
    this.div2.style.position = 'absolute';
    this.div2.style.width = '100%';
    this.div2.style.height = '100%';
    this.div2.style.zIndex = '0';
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

ItsElectric.prototype.requery = function() {
    var query;
    var queryURL = this.url;
    var extendChar = '?';
    if(this.range!==null) {
        if(this.range.start.getTime() == this.minimum && this.range.end.getTime() == this.maximum) {
            this.range = null;
        }
        else {
            queryURL = queryURL + extendChar +
                       'start='+ Math.floor(this.range.start.getTime()/1000) +
                       '&end=' + Math.floor(this.range.end.getTime()/1000);
            extendChar = '&';
        }
    }
    if(this.resolution!==null) {
        queryURL = queryURL + extendChar +
                   'resolution=' + this.resolution;
        extendChar = '&';
    }
    query = new google.visualization.Query(queryURL);
    if(this.busyId!==null) document.getElementById(this.busyId).style.display="";
    var self = this;
    query.send(function(response) {self.handleQueryResponse(response);});
};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        if(this.busyId!==null) document.getElementById(this.busyId).style.display="none";
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var realTimeNeedsAdjust = this.realTime && this.range!==null && this.range.end.getTime() == this.maximum;

    var data = response.getDataTable();
    this.minimum = data.getValue(0,0).getTime();
    this.maximum = data.getValue(data.getNumberOfRows()-1,0).getTime();
    this.timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
    var wmax = null;
    if(this.getWMax!==null) wmax = this.getWMax();
    var options = {displayAnnotations: false, displayExactValues: true,
                   allValuesSuffix: 'W'};
    var start = this.minimum;
    var end = this.maximum;
    if(this.range!==null) {
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
    var startDate = new Date();
    startDate.setTime(start + this.timeZoneOffset*1000);
    var endDate = new Date();
    endDate.setTime(end + this.timeZoneOffset*1000);
    options.zoomStartTime = startDate;
    options.zoomEndTime = endDate;
    if(wmax!==null && wmax!='') {
        options.max = wmax;
    }
    this.annotatedtimeline2.draw(data, options);
    this.resolutionString = data.getTableProperty('resolutionString');

    if(this.noFlashEvents) this.readyHandler(null);
};

ItsElectric.prototype.readyHandler = function(e) {
    if(this.busyId!==null) document.getElementById(this.busyId).style.display="none";
    if(this.resolutionId!==null) {
        var obj = document.getElementById(this.resolutionId);
        while(obj.firstChild) obj.removeChild(obj.firstChild);
        obj.appendChild(document.createTextNode(this.resolutionString));
    }

    this.ready = true;
    var temp = this.annotatedtimeline2;
    this.annotatedtimeline2 = this.annotatedtimeline;
    this.annotatedtimeline = temp;
    temp = this.div1.style.zIndex;
    this.div1.style.zIndex = this.div2.style.zIndex;
    this.div2.style.zIndex = temp;

    if(this.firstTime && !this.noFlashEvents) {
        this.firstTime = false;
        this.zoom(4*60*60);
    }
};

ItsElectric.prototype.rangeChangeHandler = function(e) {
    var oldRange = 0;
    if(this.range !== null) {
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
    newStart.setTime(this.range.end.getTime() - t*1000);
    if(newStart.getTime()<this.minimum) newStart.setTime(this.minimum);
    var newEnd = new Date();
    newEnd.setTime(this.range.end.getTime());
    this.range.start.setTime(newStart.getTime());
    this.annotatedtimeline.setVisibleChartRange(newStart,newEnd);
    var self = this;
    setTimeout(function(){self.requery();},500);
};

ItsElectric.prototype.scrollToPresent = function() {
    if(this.noFlashEvents) alert("This won't work with noFlashEvents=true.");
    if(!this.ready || this.noFlashEvents) return;
    this.range = this.annotatedtimeline.getVisibleChartRange();
    var size = this.range.end.getTime() - this.range.start.getTime();
    this.range.end.setTime(this.maximum - this.timeZoneOffset*1000);
    this.range.start.setTime(this.maximum - this.timeZoneOffset*1000 - size);
    this.annotatedtimeline.setVisibleChartRange(this.range.start,this.range.end);
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
    if(this.range!==null && this.range.end.getTime() < this.maximum) return;
    this.requery();
};
