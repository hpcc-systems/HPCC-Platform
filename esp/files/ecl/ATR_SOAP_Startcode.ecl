/*--SOAP--
<message name="PeopleFileSearchService">
  <part name="FirstName" type="xsd:string"/>
  <part name="LastName" type="xsd:string" required="1"/>
  <part name="State" type="xsd:string"/>
  <part name="Sex" type="xsd:string"/>
 </message>
*/
/*--INFO-- 
This service searches the People file. 
The LastName field is required, all others are optional.
USE ALL CAPS */

EXPORT PeopleFileSearchService() := MACRO

STRING30 fname_value := '' : STORED('FirstName');
STRING30 lname_value := '' : STORED('LastName');
STRING2 state_value  := '' : STORED('State');
STRING1 sex_value    := '' : STORED('Sex');
