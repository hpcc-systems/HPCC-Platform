#!/bin/bash

ansible-playbook /opt/hpcc-tools/ansible/stop_hpcc.yaml --extra-vars "hosts=non-dali"
ansible-playbook /opt/hpcc-tools/ansible/stop_hpcc.yaml --extra-vars "hosts=dali"
