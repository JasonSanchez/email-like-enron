#!/bin/bash

#This needs to be executed on the spak master
#This also assumes that the spark cluster was set
#up using the spark-ec2 scripts.
#this also assumes that the volumes were 
#created with the python script ./attach_enron_data.py

#Install pssh on master
yum install -y pssh git

#Grab the repo so we have the files we want
git clone git@github.com:JasonSanchez/email-like-enron.git

#mount the enron dir on the master
#this assumes that volumes have been creates
#using the python script.
mkdir /enron
mount /dev/sdh /enron

#mount the enron dir on the slaves
pssh -t 7200 -h /root/spark-ec2/slaves mkdir /enron 
pssh -t 7200 -h /root/spark-ec2/slaves mount /dev/sdh /enron

#We can also install any packages we might need
#for instance, we need libpst-libs and libpst-python to
#parse the pst files.
# http://www.five-ten-sg.com/libpst/packages/centos6/libpst-libs-0.6.66-1.el6.x86_64.rpm
# http://www.five-ten-sg.com/libpst/packages/centos6/libpst-python-0.6.66-1.el6.x86_64.rpm
# we probably don't need these on the master but just for prototyping
yum install -y http://www.five-ten-sg.com/libpst/packages/centos6/libpst-libs-0.6.66-1.el6.x86_64.rpm
yum install -y http://www.five-ten-sg.com/libpst/packages/centos6/libpst-python-0.6.66-1.el6.x86_64.rpm

pssh -t 7200 -h /root/spark-ec2/slaves yum install -y \
    http://www.five-ten-sg.com/libpst/packages/centos6/libpst-libs-0.6.66-1.el6.x86_64.rpm

pssh -t 7200 -h /root/spark-ec2/slaves yum install -y \
    http://www.five-ten-sg.com/libpst/packages/centos6/libpst-python-0.6.66-1.el6.x86_64.rpm


