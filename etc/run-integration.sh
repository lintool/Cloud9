#!/bin/sh

ant clean
ant
ant -lib etc/mail.jar integration
