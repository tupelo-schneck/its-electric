var range = null;
var annotatedtimeline;

function init() {
    annotatedtimeline = new google.visualization.AnnotatedTimeLine(
      document.getElementById('timeline'));

    google.visualization.events.addListener(annotatedtimeline, 'ready', readyHandler);
    google.visualization.events.addListener(annotatedtimeline,
                                            'rangechange', rangeChangeHandler);

    requery();
}

function requery() {
    var query;
    if(range==null) {
        query = new google.visualization.Query(itsElectricURL);
    }
    else {
        query = new google.visualization.Query(itsElectricURL + '?start='
                                               + Math.floor(range.start.getTime()/1000) +
                                               '&end='
                                               + Math.floor(range.end.getTime()/1000));
    }
    query.send(handleQueryResponse);
}

function handleQueryResponse(response) {
    if (response.isError()) {
        alert('Error in query: ' + response.getMessage() + ' ' + response.getDetailedMessage());
        return;
    }

    var data = response.getDataTable();
    if(range==null) {
        annotatedtimeline.draw(data, {'displayAnnotations': false, 'displayExactValues': true});
    }
    else {
        var timeZoneOffset = parseInt(data.getTableProperty('timeZoneOffset'));
        var newStart = new Date();
        newStart.setTime(range.start.getTime() + timeZoneOffset*1000);
        var newEnd = new Date();
        newEnd.setTime(range.end.getTime() + timeZoneOffset*1000);
        annotatedtimeline.draw(data, {'displayAnnotations': false, 'displayExactValues': true,
                    'zoomStartTime': newStart, 'zoomEndTime': newEnd});
    }
}

function readyHandler(e) {
    range = null;
}

function rangeChangeHandler(e) {
    range = annotatedtimeline.getVisibleChartRange();
    requery();
}
