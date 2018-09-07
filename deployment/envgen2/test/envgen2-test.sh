#!/bin/bash

bin_dir=/home/ming/work/HPCC_Builds/envgen2/build-new/Debug/bin
# Create single node environment.xml
# ./envgen2 -env-out ~/tmp/environment.xml -ip .

# Create a Dali node
# ./envgen2  -env-out ~/tmp/environment.xml -ip . -mod-node dali#mydali@ip=1.0.0.20
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#myroxie@ip=1.0.0.22
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#myroxie@ip=1.0.0.22;1.0.30.100
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#myroxie@ip=1.0.0.22-23
#  ipfile=~/tmp/ipfile_1 doesn't work. Can't find the file
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#myroxie@ipfile=/home/ming/tmp/ipfile_1
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#myroxie@ipfile=/home/ming/tmp/ipfile_2


# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node roxie#newroxie@ip=1.0.0.22-23
# ./envgen2 -env-in /tmp/environment_1.xml -env-out ~/tmp/environment_2.xml  -add-node thor#mythor:master@ip=1.0.0.26:slave@ip=1.0.0.27-28
# ./envgen2 -env-in /home/ming/tmp/environment_2.xml -env-out ~/tmp/environment_3.xml  -add-node eclagent#myeclagent2@ip=1.20.10.10 -add-node eclscheduler#myeclscheduler2@ip=1.20.10.20 -add-node eclccserver#myeclccserver2@ip=1.20.10.30

# The simple to create dedicated group ip to component use envgen. It will automically create topology.
# To use envgen2 you need provide "-add-topology"
# We will probably will add back old "assign_ips" in future to handle this.

#JIRA HPCC-15636 allow support node be 0 if all node assigned explicitly
# ${bin_dir}/envgen2 -show-input-only -env-in /tmp/envgen2_test/no_instance.xml -env-out /tmp/envgen2_test/manually_add_all.xml  \
 ${bin_dir}/envgen2 -env-in /tmp/envgen2_test/no_instance.xml -env-out /tmp/envgen2_test/manually_add_all.xml  \
   -add-node dali#mydali@ip=1.0.0.21 \
   -add-node dfu#mydfuserver@ip=1.0.0.22 \
   -add-node agent#myeclagent@ip=1.0.0.22 \
   -add-node scheduler#myeclscheduler@ip=1.0.0.22 \
   -add-node eclcc#myeclccserver@ip=1.0.0.23 \
   -add-node dropzone#mydropzone@ip=1.0.0.23 \
   -add-node esp#myesp@ip=1.0.0.24 \
   -add-node roxie#myroxie@ip="1.0.0.25;1.0.0.26" \
   -add-node thor#mythor:master@ip=1.0.0.27:slave@ip="1.0.0.27;1.0.0.28" \
   -add-topology topology:cluster@name=hthor:eclagent@process=myeclagent:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver \
   -add-topology topology:cluster@name=thor:eclagent@process=myeclagent:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:thor@process=mythor \
   -add-topology topology:cluster@name=roxie:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:roxie@process=myroxie \
   -add-topology topology:cluster@name=thor_roxie:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver:roxie@process=myroxie:thor@process=mythor

#${bin_dir}/envgen2 -show-input-only -env-in /tmp/envgen2_test/no_instance.xml -env-out /tmp/envgen2_test/manually_add_all.xml  \
#   -add-topology topology:cluster@name=hthor:eclagent@process=myeclagent:eclscheduler@process=myeclscheduler:eclccserver@process=myeclccserver


# JIRA HPCC-15638 allow ipfile for component assignment
# JIRA HPCC-8916 allow more than one roxie or thor cluster ip assignment
# JIRA HPCC-8915 ability to group IP address usage for envgen
# ./envgen2 -env-out ~/tmp/environment.xml -ip 1.0.0.10 -add-node roxie#roxie2@ipfile=myfile
#   -add-topology topology:cluster@name=roxie2, ...
