#!/bin/sh

export ANT_HOME=/fs/clip-qa/packages/apache-ant-1.6.2/

svn up
ant clean
ant
ant --noconfig integration


