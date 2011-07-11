/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

ReadMeText := DATASET([
 {'Before doing anything, you must edit the Setup definitions.'},
 {'These define the size of the cluster to certify by multiplication and filenames to use.'},
 {'Set each "NodeMult" value from 1-20 so their result is the number of nodes.'},
 {'The Maximum is 400-nodes (20 * 20).'},
 {'NodeMult1 should be the larger number and the two numbers should be close.'},
 {'For example, for 40-nodes, 8 and 5 are optimal (10 and 4, or 20 and 2 work, too).'},
 {'Once set, execute the certification in the following order:'},
 {'1. Run BuildDataFiles'},
 {'     -- Open the attribute in a builder window, select thor for the target, and press submit'}, 
 {'     -- builds data for steps 2, 3 & 4'},
 {'2. Run Certify_DR'},
 {'     -- Open the attribute in a builder window, select thor for the target, and press submit'}, 
 {'     -- runs processes on the data'},
 {'3. Run Build_index'},
 {'     -- Open the attribute in a builder window, select thor for the target, and press submit'}, 
 {'     -- builds an index file'},
 {'4. Run read_index'},
 {'     -- Open the attribute in a builder window, select thor for the target, and press submit'}, 
 {'     -- uses the index on Thor'},
 {'5. Access the ReadIndexService function through the ESP SOAP interface'},
 {'     -- Open the attribute in a builder window, select hthor for the target, and press compile'}, 
 {'     -- Select the EclWatch tab, and press Publish to add the compiled workunit to a QuerySet'}, 
 {'     -- Open a browser to the WsECL page (usually port 8002 of the ESP machine'}, 
 {'     -- Select the hthor querySet and click on the ReadIndexService link'}, 
 {'     -- enter "BRYANT"'},
 {'6. Compare the result of step 5 to the result of step 4'},
 {'     -- they should be the same'},
 {'7. Using ECL Watch, De-Spray the "~certification::full_test_distributed" data file.'},
 {'     -- name the de-sprayed file "/desprayed_full_test_file"'},
 {'8. Using ECL Watch, Spray Fixed the "/desprayed_full_test_file"'},
 {'     -- record length is 47'},
 {'     -- name the sprayed file "~certification::re_sprayed_file"'},
 {'     -- spray another file and name it "~certification::full_test_distributed_sprayed"'},
 {'9. Run spray_verification'},
 {'     -- Open the attribute in a builder window, select thor for the target, and press submit'}, 
 {'     -- verifies the spray occurred correctly'}
],{STRING100 line});

OUTPUT(ReadMeText);
