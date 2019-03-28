#!/bin/bash

javaPath="../example/A4/src/link/*.java"

rm -Rf ../a4User
mkdir -p ../a4User
ls ${javaPath} | xargs javac -d ../a4User -cp ../libs/*:../libs/third-party/lib/*
cd ../a4User; rm -Rf output; mkdir output; aws s3 cp s3://hua9/output ./output/ --recursive; java -cp ./ link.CheapAirlineChainLink $1 /home/radioer/CS6240/A09/a4User/output;

