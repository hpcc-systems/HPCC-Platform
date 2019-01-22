/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

ESDL OrganizationInfo Result Comparison Example

To create a new monitoring template:
-----------------------------------------------------------------------------------------------------------------
  1. Note that a working comparison template is provided.  The file name is "template1.xml".
     These are the steps that were used to create a new template which will be named
     "monitor_template_OrganizationInfo.xml".
  2. Run the following command:
    esdl monitor-template organization.esdl Organizations OrganizationInfo
	  "esdl": the command line tool.
	  "monitor-template": the command that takes the service ESDL definition and generates a new blank template.
	  "organization.esdl": the service ESDL file for this example.
	  "Organizations": The service name (defined in the ESDL file).
	  "OrganizatonInfo": The service method we are creating a result monitor for.
  3. For the template to function you need to add some attributes.  See template1.xml for examples.
     "diff_match": For any child datasets in the response there will be an empty "diff_match"
      attribute defined.
	   In order for records to be matched this must be filled in.
	   The value cannot be arbitrary.  It must be defined to match exactly how the query being monitored works.
           It's very important that this match the logic in the query exactly.
	   For our example the match for addresses is defined as:
         <Addresses diff_match='Line1+City+State'>
		   <Address>
		     <type/>
			 <Line1/>
			 <Line2/>
			 <City/>
			 <State/>
			 <Zip/>
		   </Address>
		 </Addresses>
           In our case Line2, Zip, and Zip4 are not included simply so that we can play with non-key fields
           within this example.

	   This implies that the fields that make an address unique are type, Line1, City, State,
           and Zip.

     "diff_monitor": this creates a named category of fields to be monitored.
	   In our example we've added to categories that can be monitored.  "Name" and "Address".
	   These will show up in the monitoring query as boolean selections that the user can enable or disable.
	   For example: <MonitorAdress>1</MonitorAddress> would indicate that the user wants to be alerted when address items change.

	 Nested diff_monitor.  A diff_monitor is by default inherited by its children. So in our example, the following implies that
	   all fields under "Name" will be monitored if MonitorName was selected:
       <Name diff_monitor="Name">
		  <First/>
		  <Last/>
		  <Aliases>
			<Alias/>
		  </Aliases>
		</Name>

	  - Turning off monitoring:
		If instead we didn't want aliases to be monitored just because MonitorName was enabled, we could set diff_monitor="" to disable everything under Aliases
		<Name diff_monitor="Name">
		  <First/>
		  <Last/>
		  <Aliases diff_monitor="">
			<Alias/>
		  </Aliases>
		</Name>

	  - Interleaved monitoring flags:
		The following would monitor changes to First or Last name if "MonitorName" is enabled.  And changes to the list of Aliases if "MonitorAlias" is enabled.
		<Name diff_monitor="Name">
		  <First/>
		  <Last/>
		  <Aliases diff_monitor="Alias">
			<Alias/>
		  </Aliases>
		</Name>

	  - Multiple monitoring flags:
		The following would monitor changes to All the fields under Name "MonitorName" is enabled.
		But if MonitorName is not enabled, but "MonitorAlias" is enabled, then only aliases will be monitored.
		This comes in very handy for example if I want to be able to monitor either addresses that appear anywhere, or Everything in the Best section.
		<Name diff_monitor="Name">
		  <First/>
		  <Last/>
		  <Aliases diff_monitor="Name|Alias">
			<Alias/>
		  </Aliases>
		</Name>

To generate the monitoring query from the completed template:
-----------------------------------------------------------------------------------------------------------------
  Fill in monitoring attributes in the newly created template.. or simply use template1.xml on the command below instead.
  Run the following command:
    esdl monitor organization.esdl Organizations OrganizationInfo template1.xml

    or

    esdl monitor organization.esdl Organizations OrganizationInfo monitor_template_OrganizationInfo.xml

	  "esdl": the command line tool.
	  "monitor": the command that takes the service ESDL definition and the monitoring template and generates ECL code.
	  "organization.esdl": the service ESDL file for this example.
	  "Organizations": The service name (defined in the ESDL file).
	  "OrganizationInfo": The service method we are creating a result monitor for.
	  "monitor_template_EchoPersonInfo.xml": the name of the completed monitoring template that defines the behavior of the result monitor queries.

	This command generates several ecl files, the one we are most concerned with for demo purposes is:
	  "Compare_OrganizationInfo.ecl": This is the query that will compare two input datasets and give us a result comparion document.
             This will make it easy to play with the comparison inputs and see how it affects the results.

	The generated queries most useful for monitoring applications would be used to create and run a monitor:
	  "Monitor_create_OrganizationInfo.ecl": This is the query that will create an instance of a result monitor for our "OrganizationInfo" query.
	  "Monitor_run_OrganizationInfo.ecl": This is the query that will check for any changes that have occured since the last run of our monitor.

Publishing our Monitoring queries:
-----------------------------------------------------------------------------------------------------------------
  Run the commands:
    ecl publish --server=<EclWatchIP> <RoxieTargetName> Compare_OrganizationInfo.ecl
    ecl publish --server=<EclWatchIP> <RoxieTargetName> Monitor_create_OrganizationInfo.ecl
    ecl publish --server=<EclWatchIP> <RoxieTargetName> Monitor_run_OrganizationInfo.ecl


Testing our example:
--------------------------------------------------------------------------------------------------------------
To test our comparison query we can simple navigate to the WsEcl page, populate the original result, the modified result and run the query.
For viewing in a browser it may be best to open the form outside of any frames by:
    A. selecting the appropriate query.
    B. Selecting the links tab.
    C. Clicking on "Form"
Now that the form is open on it's own frame:
    A. Check the sections you want to compare.
    B. Populate the original and changed datasets. The files result1.xml and result2.xml are provided as content for this.
    C. At the bottom of the form select 'Output XML' from the drop down and press submit.
    C. A marked up xml document should show up as the result indicating the results of the comparison.


To test using the queries for monitoring we need a back end query to run the result monitoring against.
----------------------------------------------------------------------------------------------------------------------
To be documented.

Running our tests:
----------------------------------------------------------------------------------------------------------------------
To be documented.

Adding custom ECL code to our monitoring queries:
-------------------------------------------------------------------------------
to be documented.
