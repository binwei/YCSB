#!/bin/sh
ps aux | grep couchbase | awk '{print($2)}' | sudo xargs kill -9
sudo rpm --erase couchbase-server
sudo rm -rf /raid/var/lib/couchbase/