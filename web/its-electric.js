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

function ItsElectric(url,divId1,divId2,getWMax,resolutionId) {
    this.ready = false;
    this.firstTime = true;
    this.url = url;
    this.divId1 = divId1;
    this.divId2 = divId2;
    this.getWMax = getWMax;
    this.ready = false;
    this.resolution = null;
    this.resolutionId = resolutionId;
    this.resolutionString = "";
    this.minimum = 0;
    this.maximum = 0;
    this.realTime = true;
    var self = this;
    setInterval(function(){self.realTimeUpdate();},60000);
}

ItsElectric.prototype.init = function() {
    this.annotatedtimeline = new google.visualization.AnnotatedTimeLine(
        document.getElementById(this.divId1));
    this.annotatedtimeline2 = new google.visualization.AnnotatedTimeLine(
        document.getElementById(this.divId2));
    this.range = null;

    var self = this;
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'ready',
                                            function(e){self.readyHandler(e)});
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e)});
    google.visualization.events.addListener(this.annotatedtimeline2,
                                            'ready',
                                            function(e){self.readyHandler(e)});
    google.visualization.events.addListener(this.annotatedtimeline2,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e)});
    this.requery();
};

ItsElectric.prototype.requery = function() {
    var query;
    var queryURL = this.url;
    var extendChar = '?';
    if(this.range!=null) {
        if(this.range.start.getTime() == this.minimum && this.range.end.getTime() == this.maximum) {
            this.range = null;
        }
        else {
            queryURL = queryURL + extendChar 
                       + 'start='+ Math.floor(this.range.start.getTime()/1000)
                       + '&end=' + Math.floor(this.range.end.getTime()/1000);
            extendChar = '&';
        }
    }
    if(this.resolution!=null) {
        queryURL = queryURL + extendChar 
                   + 'resolution=' + this.resolution;
        extendChar = '&';    
    }
    query = new google.visualization.Query(queryURL);
    document.getElementById('busy').style.display="";
    var self = this;
    query.send(function(response) {self.handleQueryResponse(response)});
};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        document.getElementById('busy').style.display="none";
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var realTimeNeedsAdjust = this.realTime && this.range!=null && this.range.end.getTime() == this.maximum;

    var data = response.getDataTable();
    this.minimum = data.getValue(0,0).getTime();
    this.maximum = data.getValue(data.getNumberOfRows()-1,0).getTime();
    this.timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
    var wmax = this.getWMax();
    var options = {displayAnnotations: false, displayExactValues: true,
                   allValuesSuffix: 'W'};
    var start = this.minimum;
    var end = this.maximum;
    if(this.range!=null) {
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
    if(wmax!=null && wmax!='') {
        options.max = wmax;
    }
    this.annotatedtimeline2.draw(data, options);
    this.resolutionString = data.getTableProperty('resolutionString');
};

ItsElectric.prototype.readyHandler = function(e) {
    document.getElementById('busy').style.display="none";
    document.getElementById(this.resolutionId).innerHTML = this.resolutionString;

    this.ready = true;
    var temp = this.annotatedtimeline2;
    this.annotatedtimeline2 = this.annotatedtimeline;
    this.annotatedtimeline = temp;
    temp = document.getElementById(this.divId1).style.zIndex;
    document.getElementById(this.divId1).style.zIndex = document.getElementById(this.divId2).style.zIndex;
    document.getElementById(this.divId2).style.zIndex = temp;
    
    if(this.firstTime) {
        this.firstTime = false;
        this.zoom(4*60*60);
    }
};

ItsElectric.prototype.rangeChangeHandler = function(e) {
    var oldRange = 0;
    if(this.range != null) {
        oldRange = this.range.end.getTime() - this.range.start.getTime();
    }
    this.range = this.annotatedtimeline.getVisibleChartRange();
    if(this.range.end.getTime() - this.range.start.getTime() != oldRange) {
        this.resolution = null;
    }
    this.requery();
};

ItsElectric.prototype.zoom = function(t) {
    if(!this.ready) return;
    this.range = this.annotatedtimeline.getVisibleChartRange();
    this.resolution = null;
    var newStart = new Date();
    newStart.setTime(this.range.end.getTime() - t*1000);
    var newEnd = new Date();
    newEnd.setTime(this.range.end.getTime());    
    this.range.start.setTime(newStart.getTime());
    this.annotatedtimeline.setVisibleChartRange(newStart,newEnd);
    var self = this;
    setTimeout(function(){self.requery()},500);
};

ItsElectric.prototype.setResolution = function(t) {
    if(!this.ready) return;
    this.resolution = t;
    this.requery();
};

ItsElectric.prototype.realTimeUpdate = function() {
    if(!this.ready) return;
    if(!this.realTime) return;
    if(this.range!=null && this.range.end.getTime() < this.maximum) return;
    this.requery();
}; 