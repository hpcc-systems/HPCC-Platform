#!/bin/bash
[[ ! -e id_rsa ]] && ssh-keygen -q -f id_rsa -t rsa -N ''
[[ ! -e id_rsa.pub ]] && ssh-keygen -y -f id_rsa > id_rsa.pub && chmod 0644 id_rsa.pub
t_pubkey=$(cat id_rsa.pub)
grep ^${t_pubkey}$ authorized_keys >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    cat id_rsa.pub >> authorized_keys
    chmod 0600 authorized_keys
fi
