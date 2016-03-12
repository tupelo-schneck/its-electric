#!/bin/bash

export where=test

if false
then export where=its-electric
fi

ant clean

mkdir -p build/web
sed -e '/<!--INSERT PERSONAL TEXT HERE-->/ rlocalweb/local.html' \
    -e '/>v1.10.2</ s/v1.10.2/<a href="NEWS.txt">v1.10.2<\/a>/' \
    web/its-electric.html \
    > build/web/its-electric.html
sed -e '/datasourceURL:/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/hasVoltage:/ s/false/true/' \
    -e '/hasKVA:/ s/false/true/' \
    web/its-electric-config.js \
    > build/web/its-electric-config.js
sed -e "/<\/body>/ i\\
    <script type='text/javascript'>var val = document.getElementById('wmax').value;if(!val || val=='')document.getElementById('wmax').value = 7000;</script>" \
    -e '/its-electric-config.js/ s/its-electric-config.js/its-electric-full-screen-config.js/' \
    web/its-electric-full-screen.html \
    > build/web/its-electric-full-screen.html
sed -e '/datasourceURL:/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/hasVoltage:/ s/false/true/' \
    -e '/hasKVA:/ s/false/true/' \
    -e '/partialRange:/ s/false/true/' \
    -e '/realTimeUpdateInterval:/ s/60000/10000/' \
    web/its-electric-config.js \
    > build/web/its-electric-full-screen-config.js
sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    web/its-electric-history.html \
    > build/web/its-electric-history.html
sed -e "/INSERT DATA-ADJUSTING CODE/ i\\
    for(var i = 0; i < numRows; i++) { data.setValue(i,1,Math.max(0,data.getValue(i,1)-Math.max(0,data.getValue(i,3)+200))); data.setValue(i,2,Math.max(0,data.getValue(i,2)-Math.max(0,data.getValue(i,4)-280))); }" \
    web/its-electric.js \
    > build/web/its-electric.js
cp web/* /Library/WebServer/Documents/$where
cp NEWS.txt /Library/WebServer/Documents/$where
cp build/web/* /Library/WebServer/Documents/$where
ant
cp build/its-electric-*.zip /Library/WebServer/Documents/$where
