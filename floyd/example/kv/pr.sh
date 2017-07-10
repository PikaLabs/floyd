#!/bin/sh

#####################
# sdk.proto
###################
FILE3="sdk"
protoc -I=./ --cpp_out=./ ./$FILE3.proto
