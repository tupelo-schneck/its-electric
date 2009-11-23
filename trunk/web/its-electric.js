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

function ItsElectric(url,divId,getWMax,resolutionId) {
    this.url = url;
    this.divId = divId;
    this.getWMax = getWMax;
    this.ready = false;
    this.resolution = null;
    this.resolutionId = resolutionId;
}

ItsElectric.prototype.init = function() {
    this.annotatedtimeline = new google.visualization.AnnotatedTimeLine(
      document.getElementById(this.divId));
    this.range = null;

    var self = this;
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'ready',
                                            function(e){self.readyHandler(e)});
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e)});
    this.requery();
};

ItsElectric.prototype.requery = function() {
    var query;
    var queryURL = this.url;
    var extendChar = '?';
    if(this.range!=null) {
        queryURL = queryURL + extendChar 
                   + 'start='+ Math.floor(this.range.start.getTime()/1000)
                   + '&end=' + Math.floor(this.range.end.getTime()/1000);
        extendChar = '&';
    }
    if(this.resolution!=null) {
        queryURL = queryURL + extendChar 
                   + 'resolution=' + this.resolution;
        extendChar = '&';    
    }
    query = new google.visualization.Query(queryURL);
    var self = this;
    query.send(function(response) {self.handleQueryResponse(response)});
};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var data = response.getDataTable();
    this.timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
    var wmax = this.getWMax();
    var options = {displayAnnotations: false, displayExactValues: true,
                   allValuesSuffix: 'W'};
    if(this.range!=null) {
        var newStart = new Date();
        newStart.setTime(this.range.start.getTime() + this.timeZoneOffset*1000);
        var newEnd = new Date();
        newEnd.setTime(this.range.end.getTime() + this.timeZoneOffset*1000);
        options.zoomStartTime = newStart;
        options.zoomEndTime = newEnd;
    }
    if(wmax!=null && wmax!='') {
        options.max = wmax;
    }
    this.annotatedtimeline.draw(data, options);
    document.getElementById(this.resolutionId).innerHTML = data.getTableProperty('resolutionString');
};

ItsElectric.prototype.readyHandler = function(e) {
    this.ready = true;
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