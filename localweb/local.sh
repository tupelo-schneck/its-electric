#!/bin/bash

sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/<!--INSERT PERSONAL TEXT HERE-->/ rlocalweb/local.html' web/its-electric.html \
    > build/its-electric.html
cp build/its-electric.html /Library/WebServer/Documents
cp web/its-electric.js /Library/WebServer/Documents
ant
cp build/its-electric.zip /Library/WebServer/Documents