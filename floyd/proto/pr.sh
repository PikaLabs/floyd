#!/bin/sh

###################
# floyd.proto
###################
FILE1="floyd"
for file in $FILE1 ; do
  protoc -I=./ --cpp_out=./ ./$file.proto
  cp $file.pb.h $file.pb.cc ../src
  cp $file.pb.h $file.pb.cc ../tools/log_parse
  mv $file.pb.h $file.pb.cc ../tools/floyd_upgrade
done
