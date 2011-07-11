## Copyright © 2011 HPCC Systems.  All rights reserved. 
/LocalEnvFile/ && ( NEW_ENVFILE != "" ) { gsub("${CONFIG_DIR}/${ENV_XML_FILE}", NEW_ENVFILE )} 
/LocalConfFile/ && ( NEW_CONFFILE != "" ) { gsub("${CONFIG_DIR}/${ENV_CONF_FILE}", NEW_CONFFILE )  }
/EspBinding/ && ( NEW_PORT != "" )  { gsub(/port=\"[0-9]*\"/, "port=\""NEW_PORT  "\"" )  }

{ print $0 } 
