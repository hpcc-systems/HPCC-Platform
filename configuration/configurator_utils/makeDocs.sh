mkdir ./Docs

./configurator -doc -use dafilesrv.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use dali.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs

./configurator -doc -use dfuplus.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use dfuserver.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs

./configurator -doc -use eclagent_config.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use eclccserver.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs

./configurator -doc -use eclscheduler.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use esp.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs

./configurator -doc -use ftslave_linux.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use ldapserver.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs/

./configurator -doc -use roxie.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
./configurator -doc -use sasha.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs

./configurator -doc -use thor.xsd -b /opt/HPCCSystems/componentfiles/configxml/ -t ./Docs
