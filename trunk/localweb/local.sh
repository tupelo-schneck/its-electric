#!/bin/bash

export where=test

sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/<!--INSERT PERSONAL TEXT HERE-->/ rlocalweb/local.html' web/its-electric.html \
    > build/its-electric.html
cp build/its-electric.html /Library/WebServer/Documents/$where
cp web/its-electric.js /Library/WebServer/Documents/$where
ant
cp build/its-electric-*.zip /Library/WebServer/Documents/$where
