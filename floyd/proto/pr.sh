#!/bin/sh
set -x

protoc -I=./ --cpp_out=../src/ ./floyd.proto

echo "run protoc success, go, go, go...";
