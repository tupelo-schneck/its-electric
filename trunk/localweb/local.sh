#!/bin/bash

export where=test3

if false
then export where=its-electric; rm -rf main/* build/*
fi

mkdir -p build/web
sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/<!--INSERT PERSONAL TEXT HERE-->/ rlocalweb/local.html' \
    -e '/>v1.5</ s/v1.5/<a href="NEWS.txt">v1.5<\/a>/' \
    web/its-electric.html \
    > build/web/its-electric.html
sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e "/<\/body>/ i\\
    <script type='text/javascript'>var val = document.getElementById('wmax').value;if(!val || val=='')document.getElementById('wmax').value = 10000;</script>" \
    web/its-electric-full-screen.html \
    > build/web/its-electric-full-screen.html
sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    web/its-electric-history.html \
    > build/web/its-electric-history.html
cp web/* /Library/WebServer/Documents/$where
cp NEWS.txt /Library/WebServer/Documents/$where
cp build/web/* /Library/WebServer/Documents/$where
ant
cp build/its-electric-*.zip /Library/WebServer/Documents/$where
