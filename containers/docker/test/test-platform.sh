#!/bin/bash

#How to run:
# "--privilged" is important on CentOS. Otherwise "su - hpcc" will fail with error
# "su: cannot open session: Permission denied"
#sudo docker run --privileged --rm -v "$PWD/test-platform.sh:/usr/local/bin/test.sh" hpccsystems/hpcc:<tag> test.sh


cd ~
mkdir -p tmp
cd tmp

# Start HPCC
/etc/init.d/hpcc-init start > /dev/null 2>&1
rc=$?
if [ $rc -ne 0 ]; then
  echo "Start HPCC failed Error code: $rc"
  exit 1
fi


cat > test.ecl << EOF
/*
    Example code - use without restriction.
*/
Layout_Person := RECORD
  UNSIGNED1 PersonID;
  STRING15  FirstName;
  STRING25  LastName;
END;

allPeople := DATASET([ {1,'Fred','Smith'},
                       {2,'Joe','Blow'},
                       {3,'Jane','Smith'}],Layout_Person);

somePeople := allPeople(LastName = 'Blow');

//  Outputs  ---
somePeople;
EOF

# Test ecl code through esp
/opt/HPCCSystems/bin/ecl run hthor test.ecl > test_result 2>&1
rc=$?
if [ $rc -ne 0 ]; then
  echo "Test ecl code through esp failed! Error code: $rc"
  exit 2
fi

# Validate result
cat test_result | grep "<Row><personid>2</personid><firstname>Joe[[:space:]]*</firstname><lastname>Blow[[:space:]]*</lastname></Row>" test_result > /dev/null
if [ $rc -ne 0 ]; then
  echo "Test result validate failed!"
  exit 3
fi

echo "ECL test through esp succeeded!"
