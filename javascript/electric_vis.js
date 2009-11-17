
var range = null;
var annotatedtimeline;

function init() {
    annotatedtimeline = new google.visualization.AnnotatedTimeLine(
      document.getElementById('visualization'));

    google.visualization.events.addListener(annotatedtimeline, 'ready', readyHandler);
    google.visualization.events.addListener(annotatedtimeline,
                                            'rangechange', rangeChangeHandler);

    requery();
}

function requery() {
    var query;
    if(range==null) {
        query = new google.visualization.Query('http://tupelo-schneck.org:8081');
    }
    else {
        query = new google.visualization.Query('http://tupelo-schneck.org:8081?start='
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
//                'allowRedraw': true});
    }
    else {
        var newStart = new Date();
        newStart.setTime(range.start.getTime() - newStart.getTimezoneOffset()*60000);
        var newEnd = new Date();
        newEnd.setTime(range.end.getTime() - newEnd.getTimezoneOffset()*60000);
        annotatedtimeline.draw(data, {'displayAnnotations': false, 'displayExactValues': true,
//                'allowRedraw': true});
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
