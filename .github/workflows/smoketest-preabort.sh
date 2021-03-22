echo 'List of processes:'
ps aux 
echo 'esp stacks'
echo '=========='
sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" esp `pidof esp`
echo 'daserver stacks'
echo '==============='
sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" daserver `pidof daserver`
for p in $(pidof eclcc); do
 echo 'eclcc[${p}] stacks'
 echo '==============='
 sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" eclcc ${p}
done
echo 'job queues'
echo '==============='
daliadmin . export /JobQueues jq.xml
cat jq.xml
echo '***************'
