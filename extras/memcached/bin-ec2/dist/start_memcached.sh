#!/bin/sh

export LD_LIBRARY_PATH=.
chmod +x memcached
./memcached -d -u nobody -m 1024 -p 11211

