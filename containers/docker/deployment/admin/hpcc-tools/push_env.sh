#!/bin/bash

ansible-playbook /opt/hpcc-tools/ansible/push_env.yaml --extra-vars "hosts=hpcc"
