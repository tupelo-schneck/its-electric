#!/bin/bash

export where=test

if true
then export where=.; rm -rf main/* build/*
fi

mkdir -p build
sed -e '/CHANGE URL HERE/ s/localhost:8081/tupelo-schneck.org:8081/' \
    -e '/<!--INSERT PERSONAL TEXT HERE-->/ rlocalweb/local.html' \
    -e '/>v1.4.1</ s/v1.4.1/<a href="NEWS.txt">v1.4.1<\/a>/' \
    web/its-electric.html \
    > build/its-electric.html
cp web/* /Library/WebServer/Documents/$where
cp NEWS.txt /Library/WebServer/Documents/$where
cp build/its-electric.html /Library/WebServer/Documents/$where
ant
cp build/its-electric-*.zip /Library/WebServer/Documents/$where
