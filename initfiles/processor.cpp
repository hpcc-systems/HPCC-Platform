/*
## Copyright © 2011 HPCC Systems.  All rights reserved.
*/

#include <string> 
#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

string getFileVector(const char* fileName){
    vector<string> text;
    string line;
    ifstream textstream(fileName);
    while ( getline(textstream, line) ){
        text.push_back(line + "\n");
    } 
    textstream.close();

    string alltext;
    for (int i=0; i < text.size(); i++)
        alltext += text[i];

    return alltext;
}


int main(int argc, char* argv[])
{

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <bash-vars> <input> <output>\n", argv[0]);
        return 1;
    }

    string bashVars = getFileVector(argv[1]);
    string inFileContents = getFileVector(argv[2]);
    
    string searchString( "###<REPLACE>###" );
    string replaceString( bashVars );
    string::size_type pos = 0;
    while ( (pos = inFileContents.find(searchString, pos)) != string::npos ) 
    {
        inFileContents.replace( pos, searchString.size(), replaceString );
        pos++;
    }
    ofstream outstream(argv[3]);
    outstream<<inFileContents;
    outstream.close();
    return 0;
}
