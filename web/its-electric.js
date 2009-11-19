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

function ItsElectric(url,divId) {
    this.url = url;
    this.divId = divId;
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
    if(this.range==null) {
        query = new google.visualization.Query(this.url);
    }
    else {
        query = new google.visualization.Query(this.url + '?start='
                                               + Math.floor(this.range.start.getTime()/1000) +
                                               '&end='
                                               + Math.floor(this.range.end.getTime()/1000));
    }
    var self = this;
    query.send(function(response) {self.handleQueryResponse(response)});
};

ItsElectric.prototype.handleQueryResponse = function(response) {
    if (response.isError()) {
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var data = response.getDataTable();
    if(this.range==null) {
        this.annotatedtimeline.draw(data, {'displayAnnotations': false, 'displayExactValues': true});
    }
    else {
        var timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
        var newStart = new Date();
        newStart.setTime(this.range.start.getTime() + timeZoneOffset*1000);
        var newEnd = new Date();
        newEnd.setTime(this.range.end.getTime() + timeZoneOffset*1000);
        this.annotatedtimeline.draw(data, {'displayAnnotations': false, 'displayExactValues': true,
                    'zoomStartTime': newStart, 'zoomEndTime': newEnd});
    }
};

ItsElectric.prototype.readyHandler = function(e) {
    this.range = null;
};

ItsElectric.prototype.rangeChangeHandler = function(e) {
    this.range = this.annotatedtimeline.getVisibleChartRange();
    this.requery();
};
