#!/bin/bash

ansible-playbook /opt/hpcc-tools/ansible/start_hpcc.yaml --extra-vars "hosts=dali"
ansible-playbook /opt/hpcc-tools/ansible/start_hpcc.yaml --extra-vars "hosts=non-dali"
