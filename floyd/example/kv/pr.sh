#!/bin/sh

#####################
# sdk.proto
###################
FILE3="client"
protoc -I=./ --cpp_out=./ ./$FILE3.proto
cp $FILE3.pb.h $FILE3.pb.cc ./sdk
mv $FILE3.pb.h $FILE3.pb.cc ./server
