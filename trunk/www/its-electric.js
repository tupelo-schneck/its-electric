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
