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

function ItsElectric(timelineId,busyId,resolutionId,toolbarId,columnCheckboxesId,errorCallback) {
    this.queryPath = "/power";
    this.timelineId = timelineId;
    this.busyId = busyId;
    this.resolutionId = resolutionId;
    this.toolbarId = toolbarId;
    this.columnCheckboxesId = columnCheckboxesId;
    this.errorCallback = errorCallback;
    
    this.div0 = null;
    this.div1 = null;

    this.ready = false;
    this.firstTime = true;
    this.range = null;
    this.resolution = null;
    this.resolutionString = "";
    this.minimum = 0;
    this.maximum = 0;
    this.columnChecked = [];

    this.calledDraw = false;
    this.rangeChangeTimeout = null;
    
    this.querying = false;
    this.pendingQuery = false;
    this.pendingDraw = false;
    this.errors = [];
    this.requeryBackoff = ItsElectric.minRequeryBackoff;

    this.lastTouched = new Date().getTime();
}

ItsElectric.maxErrors = 20;
ItsElectric.minRequeryBackoff = 10000;
ItsElectric.maxRequeryBackoff = 320000;

ItsElectric.prototype.configure = function(config) {
    this.datasourceURL = config.datasourceURL;
    this.partialRange = config.partialRange;
    this.initialZoom = config.initialZoom;
    this.realTime = config.realTime;
    this.realTimeUpdateInterval = config.realTimeUpdateInterval;
    this.hasVoltage = config.hasVoltage;
    this.hasKVA = config.hasKVA;
}

ItsElectric.prototype.init = function() {
    this.div0 = document.createElement('div');
    this.div0.style.position = 'relative';
    this.div0.style.width = '100%';
    this.div0.style.height = '100%';
    document.getElementById(this.timelineId).appendChild(this.div0);

    this.div1 = document.createElement('div');
    this.div1.style.position = 'absolute';
    this.div1.style.width = '100%';
    this.div1.style.height = '100%';
    this.div0.appendChild(this.div1);
    this.annotatedtimeline = new google.visualization.AnnotationChart(this.div1);

    var self = this;
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'ready',
                                            function(e){self.readyHandler(e);});
    google.visualization.events.addListener(this.annotatedtimeline,
                                            'rangechange',
                                            function(e){self.rangeChangeHandler(e);});
    this.requery();
};

ItsElectric.prototype.queryURL = function() {
    var realTimeNeedsAdjust = this.realTime && this.range && this.range.end.getTime() == this.maximum;
    var queryURL = this.datasourceURL;
    if(queryURL.charAt(queryURL.length-1)=='/' && this.queryPath.charAt(0)=='/') {
      queryURL = queryURL + this.queryPath.substring(1);
    }
    else {
      queryURL = queryURL + this.queryPath;
    }
    var extendChar = '?';
    queryURL = queryURL + extendChar + 'extraPoints=2';
    extendChar = '&';
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
                rangeEnd = Math.floor(Math.min(this.maximum/1000, rangeEnd));
                queryURL = queryURL + extendChar +
                'rangeStart=' + rangeStart +
                '&rangeEnd=' + rangeEnd;
                extendChar = '&';
            }
        }
    }
    else if(this.firstTime) {
    	queryURL = queryURL + extendChar +
            'start='+ 100000000 +
            '&end=' + (100000000 + this.initialZoom) +
            '&realTimeAdjust=yes';
        extendChar = '&';
        this.range = { start:new Date(100000000*1000), end:new Date((100000000+this.initialZoom)*1000) };
        this.maximum = (100000000 + this.initialZoom)*1000;
    }
    if(this.resolution) {
        queryURL = queryURL + extendChar +
                   'resolution=' + this.resolution;
        extendChar = '&';
    }
    return queryURL;
};

ItsElectric.prototype.toolbarQueryURL = function() {
    var queryURL = this.datasourceURL;
    if(queryURL.charAt(queryURL.length-1)=='/' && this.queryPath.charAt(0)=='/') {
      queryURL = queryURL + this.queryPath.substring(1);
    }
    else {
      queryURL = queryURL + this.queryPath;
    }
    var extendChar = '?';
    if(this.ready) {
        if(this.range && (this.range.start.getTime() != this.minimum || this.range.end.getTime() != this.maximum)) {
            var start = Math.floor(this.range.start.getTime()/1000);
            var end = Math.floor(this.range.end.getTime()/1000);
            queryURL = queryURL + extendChar +
                       'rangeStart='+ start +
                       '&rangeEnd=' + end;
            extendChar = '&';
        }
    }
    if(this.resolution) {
        queryURL = queryURL + extendChar +
                   'resolution=' + this.resolution;
        extendChar = '&';
    }
    return queryURL;
};

ItsElectric.prototype.requery = function() {
    clearTimeout(this.requeryTimeoutId);
	this.requeryTimeoutId = null;
	clearTimeout(this.realTimeUpdateTimeoutId);
	this.realTimeUpdateTimeoutId = null;
    if(this.querying) {
        this.pendingQuery = true;
        return;
    }
    this.querying = true;
    this.pendingQuery = false;
    this.query = new google.visualization.Query(this.queryURL());
    if(this.busyId) document.getElementById(this.busyId).style.display="";
    this.query.setTimeout(120);
    var self = this;
    this.query.send(function(response) {self.handleQueryResponse(response);});
    delete this.query;
};

ItsElectric.prototype.requeryAfter = function(n) {
    var self = this;
    this.requeryTimeoutId = setTimeout(function(){self.requery();},n);
};

ItsElectric.prototype.redrawAfter = function(n) {
    var self = this;
    setTimeout(function(){self.redraw();},n);
};

ItsElectric.prototype.options = {displayAnnotations: false,
                                 displayExactValues: true,
                                 allValuesSuffix: 'W',
                                 dateFormat: 'yyyy-MM-dd HH:mm:ss'};

ItsElectric.setOnclick = function(self,index,node) {
    node.onclick=function(){self.showOrHideColumn(index,node.checked);};
};

ItsElectric.prototype.logError = function(s) {
	if(this.errors.length > ItsElectric.maxErrors) {
		this.errors.pop();
	}
	this.errors.unshift("" + ItsElectric.toISOString(new Date()) + ": " + s);
	if(this.errorCallback) this.errorCallback();
};

ItsElectric.prototype.clearErrors = function() {
	this.errors = [];
};

ItsElectric.prototype.getErrors = function() {
	return this.errors;
};

ItsElectric.toISOString = function(d) {
	var padzero = ItsElectric.padZero;
	return d.getFullYear() + '-' +  padzero(d.getMonth() + 1) + '-' + padzero(d.getDate()) + ' ' + padzero(d.getHours()) + ':' +  padzero(d.getMinutes()) + ':' + padzero(d.getSeconds());
};

ItsElectric.padZero = function(n) {
	return n < 10 ? '0' + n : n;
};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        if(this.busyId) document.getElementById(this.busyId).style.display="none";
        this.logError(response.getMessage() + ' ' + response.getDetailedMessage());
        this.querying = false;
        if(this.pendingQuery) {
        	this.requeryAfter(1);
        }
        else {
        	if(this.pendingDraw) this.redrawAfter(1);
        	this.requeryAfter(this.requeryBackoff);
        	this.requeryBackoff *= 2;
        	if(this.requeryBackoff > ItsElectric.maxRequeryBackoff) this.requeryBackoff = ItsElectric.maxRequeryBackoff;
        }
        return;
    }
    this.requeryBackoff = ItsElectric.minRequeryBackoff;

    var realTimeNeedsAdjust = this.realTime && this.range && this.range.end.getTime() == this.maximum;

    var data = response.getDataTable();
    var numRows = data.getNumberOfRows();
    var numCols = data.getNumberOfColumns();

    // INSERT DATA-ADJUSTING CODE
    
//// Example: this code sums all columns     
//    for(var i = 0; i < numRows; i++) { 
//    	var sum = 0;
//    	for(var j = 1; j < numCols; j++) {
//    		sum += data.getValue(i,j);
//    	}
//    	data.setValue(i,1,sum);
//    }
//    data.setColumnLabel(1,"Total");
//    data.removeColumns(2,numCols - 1);
//    numCols = 2;
    
    if(this.columnCheckboxesId!=null) {
        var obj = document.getElementById(this.columnCheckboxesId);
        while(obj.firstChild) obj.removeChild(obj.firstChild);
        for(var i = 1; i < numCols; i++) {
            if(obj.firstChild) {
                obj.appendChild(document.createTextNode("\u00a0"));
            }
            var node = document.createElement("input");
            node.type = 'checkbox';
            if(this.columnChecked.length<i || this.columnChecked[i-1]!=false) {
                node.checked = true;
            }
            var index = i - 1;
            var self = this;
            ItsElectric.setOnclick(self,index,node);
            obj.appendChild(node);
            obj.appendChild(document.createTextNode(" " + data.getColumnLabel(i)));
        }
    }

    if(this.delta && numRows >= 2) {
        var prev = [];
        for(var i = 1; i < numCols; i++) {
            prev[i] = data.getValue(0,i);
            data.setValue(0,i,0);
        }
        for(var i = 1; i < numRows; i++) {
            for(var j = 1; j < numCols; j++) {
                var temp = data.getValue(i,j);
                data.setValue(i,j,temp - prev[j]);
                prev[j] = temp;
            }
        }
    }

    var rangeStart = numRows==0 ? 0 : data.getValue(0,0).getTime();
    if(isNaN(this.minimum) || this.minimum==0) {
        this.minimum = rangeStart;
    }
    var rangeEnd = numRows==0 ? 0 : data.getValue(numRows-1,0).getTime()

    // Make this.maximum be the time at which client's local time is the same clock time as the server's maximum
    this.timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
    this.maximum = parseInt(data.getTableProperty('maximum'))*1000 - this.timeZoneOffset*1000;
    if(isNaN(this.maximum) || this.maximum==0) {
        this.maximum = rangeEnd;
    }
// no longer needed for AnnotationChart?
//    else {
//        var maximumDate = new Date(this.maximum);
//        maximumDate.setTime(this.maximum + maximumDate.getTimezoneOffset()*60000);
//        maximumDate.setTime(this.maximum + maximumDate.getTimezoneOffset()*60000);
//        this.maximum = maximumDate.getTime();
//    }

    if(this.minimum!=0 && (numRows==0 || this.minimum < rangeStart)) {
        data.insertRows(0,1);
        data.setValue(0,0,new Date(this.minimum));
        for(var i = 1; i < numCols; i++) {
            data.setValue(0,i,numRows==0 ? 0 : data.getValue(1,i));
        }
    }
    if(this.maximum!=0 && (numRows==0 || this.maximum > rangeEnd)) {
        var newRow = data.addRow();
        data.setValue(newRow,0,new Date(this.maximum));
        for(var i = 1; i < numCols; i++) {
            data.setValue(newRow,i,numRows==0 ? 0 : data.getValue(newRow-1,i));
        }
    }

    if(this.range==null) this.range = { start: new Date(this.minimum), end: new Date(this.maximum) };

    if(realTimeNeedsAdjust) {
        var start = this.maximum - (this.range.end.getTime() - this.range.start.getTime());
        var end = this.maximum;
        this.range.start.setTime(start);
        this.range.end.setTime(end);
    }
    this.resolutionString = data.getTableProperty('resolutionString');
    this.currentResolution = parseInt(data.getTableProperty('resolution'));

    this.data = data;
    this.querying = false; // fake reentrant lock
    this.redraw();
    if(this.toolbarId) {
        var toolbarElement = document.getElementById(this.toolbarId);
        var toolbarQueryURL = this.toolbarQueryURL();
        google.visualization.drawToolbar(toolbarElement,
         [{type: 'html', datasource: toolbarQueryURL},
          {type: 'csv', datasource: toolbarQueryURL}])
        ItsElectric.renameChartOptions(toolbarElement);
    }
};

ItsElectric.renameChartOptions = function(element) {
    if(element.nodeType==3 && element.nodeValue=="Chart options") {
        element.nodeValue = "Export data";
        return true;
    }
    for(var i = 0; i < element.childNodes.length; i++) {
        if (ItsElectric.renameChartOptions(element.childNodes[i])) return true;
    }
    return false;
};

ItsElectric.prototype.redraw = function() {
    if(!this.data) return;
    this.lastTouched = new Date().getTime();
    if(this.querying) {
        this.pendingDraw = true;
        return;
    }
    this.querying = true;
    this.pendingDraw = false;

    if(this.busyId) document.getElementById(this.busyId).style.display="";

    var startDate = new Date();
    var endDate = new Date();
    ItsElectric.setDateAdjusted(startDate, this.range.start.getTime());
    ItsElectric.setDateAdjusted(endDate, this.range.end.getTime());
    this.options.zoomStartTime = startDate;
    this.options.zoomEndTime = endDate;
    this.calledDraw = true;
    this.annotatedtimeline.draw(this.data, this.options);
};

// this horribleness makes things behave close to daylight saving time change
ItsElectric.setDateAdjusted = function(date,time) {
    date.setTime(time);
// no longer needed for AnnotationChart?
//    date.setTime(time - date.getTimezoneOffset()*60000);
//    // yes, again, in case we are close to DST
//    date.setTime(time - date.getTimezoneOffset()*60000);
};

// zoom buttons pushed
ItsElectric.prototype.detectRangeChange = function() {
    var newRange = this.annotatedtimeline.getVisibleChartRange();
    if (this.range.start.getTime() != newRange.start.getTime()) {
        return true;
    } else {
        return false;
    }
};

ItsElectric.prototype.readyHandler = function(e) {
    this.ready = true;
    if (this.handlingRangeChange) {
        this.handlingRangeChange = false;
        return;
    }
    if (!this.querying && this.detectRangeChange()) {
        this.rangeChangeHandler();
        return;
    }
    if (!this.calledDraw) {
        return;
    }
    
    delete this.options.zoomStartTime;
    delete this.options.zoomEndTime;

    if(this.resolutionId) {
        var obj = document.getElementById(this.resolutionId);
        while(obj.firstChild) obj.removeChild(obj.firstChild);
        obj.appendChild(document.createTextNode(this.resolutionString));
    }

//    var start = this.minimum;
//    var end = this.maximum;
//    if(this.range) {
//        start = this.range.start.getTime();
//        end = this.range.end.getTime();
//    }
//    var numRows = this.data.getNumberOfRows();
//    var numCols = this.data.getNumberOfColumns();
//    var watts = [];
//    for(var j = 1; j < numCols; j++) {
//        watts[j] = 0;
//        var lastWatts = 0;
//        var lastTime = start;
//        var first = false;
//        var second = false;
//        for(var i = 0; i < numRows; i++) {
//            var thisTime = this.data.getValue(i,0).getTime();
//            if(thisTime < start || thisTime > end) continue;
//            if(this)
//            if(this.data.getValue(i,j)!=null) {
//                if(first) second = true;
//                first = true;
//                if(lastWatts!=0) {
//                    if(this.resolutionString=="1h" && j==4) alert("" + lastWatts + " " + thisTime + " " + lastTime);
//                    watts[j] += lastWatts * ((thisTime - lastTime)/1000);
//                }
//                lastWatts = this.data.getValue(i,j);
//            }
//            if(second) {
//                lastTime = thisTime;
//            }
//        }
//        watts[j] = (watts[j]/3600000).toFixed(3);
//        if(j==4) alert("" + j + " "  + watts[j]);
//    }

    this.querying = false;
    this.calledDraw = false;
    if(this.busyId) document.getElementById(this.busyId).style.display="none";
    if(this.firstTime) {
        this.firstTime = false;
        this.zoom(this.initialZoom);
    }
    if(this.pendingQuery) this.requeryAfter(1);
    else if(this.pendingDraw) this.redrawAfter(1);
    else if(this.realTimeUpdateInterval) {
        if(isNaN(this.currentResolution)) {
           if(this.resolution!=null) this.currentResolution = this.resolution;
           else this.currentResolution = 1;
        }
        this.setRealTimeUpdater();
    }
};

ItsElectric.prototype.setRealTimeUpdater = function() {
    var self = this;
    this.realTimeUpdateTimeoutId = setTimeout(function(){self.realTimeUpdate();},Math.max(this.currentResolution*1000,this.realTimeUpdateInterval));
}

ItsElectric.prototype.rangeChangeHandler = function(e) {
    var self = this;
    if (e) this.handlingRangeChange = true;
    clearTimeout(this.rangeChangeTimeout);
    if (!this.querying) {
        if(this.busyId) document.getElementById(this.busyId).style.display="";
        this.rangeChangeTimeout = setTimeout(function () { self.realRangeChangeHandler(); }, 1000);
    }
}
    
ItsElectric.prototype.realRangeChangeHandler = function(e) {
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
    if(!this.ready || !this.range) return;
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
    this.requeryAfter(500);
};

ItsElectric.prototype.scrollToPresent = function() {
    if(!this.ready || !this.range) return;
    var size = this.range.end.getTime() - this.range.start.getTime();
    var newStart = new Date();
    var newEnd = new Date();
    newEnd.setTime(this.maximum);
    this.range.end.setTime(this.maximum);
    newStart.setTime(this.maximum - size);
    this.range.start.setTime(this.maximum - size);
    this.annotatedtimeline.setVisibleChartRange(newStart,newEnd);
    this.requeryAfter(500);
};

ItsElectric.prototype.showOrHideColumn = function(col,show) {
    if(show && this.columnChecked.length<=col) return;
    this.columnChecked[col] = show;
    if(!this.ready) return;
    var self = this;
    setTimeout(function(){
        if(show) {
            self.annotatedtimeline.showDataColumns(col);
        }
        else {
            self.annotatedtimeline.hideDataColumns(col);
        }
    },1);
};

ItsElectric.prototype.setResolution = function(t) {
    if(!this.ready) return;
    this.resolution = t;
    this.requery();
};

ItsElectric.prototype.realTimeUpdate = function() {
	this.requeryTimeoutId = null;
    if(!this.ready || !this.realTime) return;
    if(this.range && this.range.end.getTime() < this.maximum) return;
    if(this.range && this.range.end.getTime() - this.range.start.getTime() == this.initialZoom*1000) {
        var now = new Date().getTime();
        if(now - this.lastTouched > 60*60*1000) {
            history.go(0);
            return;
        }
    }
    if(!this.querying) {
        this.requery();
    }
};
