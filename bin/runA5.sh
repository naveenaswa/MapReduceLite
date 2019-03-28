#!/bin/bash

javaPath="../example/A5/src/AirlineResultsParse.java"

rm -Rf ../a5User
mkdir -p ../a5User
ls ${javaPath} | xargs javac -d ../a5User -cp ../libs/*:../libs/third-party/lib/*
cd ../a5User; rm -Rf output; mkdir output; aws s3 cp s3://hua9/output ./output/ --recursive; java -cp ./ AirlineResultsParse ./output;

