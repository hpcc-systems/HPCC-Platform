#!/bin/bash
################################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
################################################################################

# Steps To resolve this
# =====================
# go to /opt/LexisNexis/lib directory
# run this script as "sh create_symlinks.sh" 

ln -s /opt/LexisNexis/lib/libMagick.so /opt/LexisNexis/lib/libMagick.so.10
ln -s /opt/LexisNexis/lib/libicudata.so /opt/LexisNexis/lib/libicudata.so.36
ln -s /opt/LexisNexis/lib/libicui18n.so /opt/LexisNexis/lib/libicui18n.so.36
ln -s /opt/LexisNexis/lib/libicule.so /opt/LexisNexis/lib/libicule.so.36
ln -s /opt/LexisNexis/lib/libicuuc.so /opt/LexisNexis/lib/libicuuc.so.36
ln -s /opt/LexisNexis/lib/libmysqlclient.so /opt/LexisNexis/lib/libmysqlclient.so.15
ln -s /opt/LexisNexis/lib/libxalan-c.so /opt/LexisNexis/lib/libxalan-c.so.110
ln -s /opt/LexisNexis/lib/libxalanMsg.so /opt/LexisNexis/lib/libxalanMsg.so.110
ln -s /opt/LexisNexis/lib/libxerces-c.so /opt/LexisNexis/lib/libxerces-c.so.27
ln -s /opt/LexisNexis/lib/libxml-security-c.so /opt/LexisNexis/lib/libxml-security-c.so.14
# libboost_regex-gcc41-mt-1_34.so not found
ln -s /opt/LexisNexis/lib/libboost_regex.so /opt/LexisNexis/lib/libboost_regex-gcc41-mt-1_34.so 
ln -s /opt/LexisNexis/lib/libboost_regex-gcc41-mt-1_34.so /opt/LexisNexis/lib/libboost_regex-gcc41-mt-1_34.so.1.34.0

