echo 'List of processes:'
ps aux 
echo 'esp stacks'
echo esp path=$(which esp)
echo eclcc path=$(which eclcc)
echo '=========='
echo sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which esp) $(pidof esp)
sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which esp) $(pidof esp)
echo 'daserver stacks'
echo '==============='
echo sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which daserver) $(pidof daserver)
sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which daserver) $(pidof daserver)
for p in $(pidof eclcc); do
 echo "eclcc[${p}] stacks"
 echo '==============='
 echo sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which eclcc) ${p}
 sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which eclcc) ${p}
done
echo 'job queues'
echo '==============='
daliadmin . export /JobQueues jq.xml
cat jq.xml
echo '***************'
