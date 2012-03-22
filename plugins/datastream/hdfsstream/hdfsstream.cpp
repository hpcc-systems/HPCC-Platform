#include "hdfs.h"
#include <string>
#include <vector>
#include <stdio.h>
#include <iostream>
#include <fstream>

using namespace std;

using std::string;
using std::vector;

//#define EOL "\n\r"
#define EOL "\n"

tOffset getBlockSize(hdfsFS * filefs, const char * filename)
{
	if (!*filefs)
	{
		fprintf(stderr, "Could not connect to hdfs");
		exit(-1);
	}

	hdfsFileInfo *fileInfo = NULL;

	if ((fileInfo = hdfsGetPathInfo(*filefs, filename)) != NULL)
	{
		tOffset bsize = fileInfo-> mBlockSize;
		hdfsFreeFileInfo(fileInfo, 1);
		return bsize;
	}
	else
	{
		fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", filename);
		exit(-1);
	}

	return 0;
}


long getFileSize(hdfsFS * filefs, const char * filename)
{
	if (!*filefs)
	{
		fprintf(stderr, "Could not connect to hdfs");
		exit(-1);
	}

	hdfsFileInfo *fileInfo = NULL;

	if ((fileInfo = hdfsGetPathInfo(*filefs, filename)) != NULL)
	{
		long fsize = fileInfo->mSize;
		hdfsFreeFileInfo(fileInfo, 1);
		return fsize;
	}
	else
	{
		fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", filename);
		exit(-1);
	}

	return 0;
}

long getRecordCount(long fsize, int clustersize, int reclen, int nodeid)
{
	long readSize = fsize / reclen / clustersize;
	if (fsize % reclen)
	{
		fprintf(stderr, "filesize (%lu) not multiple of record length(%d)", fsize, reclen);
		exit(-1);
	}
	if ((fsize / reclen) % clustersize >= nodeid + 1)
	{
		readSize += 1;
		fprintf(stderr, "\nThis node will stream one extra rec\n");
	}
	return readSize;
}

void ouputhosts(hdfsFS * fs, const char * rfile)
{
	if (!*fs)
	{
		fprintf(stderr, "Could not connect to hdfs");
		return;
	}

	getBlockSize(fs, rfile);

	char*** hosts = hdfsGetHosts(*fs, rfile, 1, 1);
	if (hosts)
	{
		fprintf(stderr, "hdfsGetHosts - SUCCESS! ... \n");
		int i = 0;
		while (hosts[i])
		{
			int j = 0;
			while (hosts[i][j])
			{
				fprintf(stderr, "\thosts[%d][%d] - %s\n", i, j, hosts[i][j]);
				++j;
			}
			++i;
		}
	}
}

void outputFileInfo(hdfsFileInfo * fileInfo)
{
	printf("Name: %s, ", fileInfo->mName);
	printf("Type: %c, ", (char) (fileInfo->mKind));
	printf("Replication: %d, ", fileInfo->mReplication);
	printf("BlockSize: %ld, ", fileInfo->mBlockSize);
	printf("Size: %ld, ", fileInfo->mSize);
	printf("LastMod: %s", ctime(&fileInfo->mLastMod));
	printf("Owner: %s, ", fileInfo->mOwner);
	printf("Group: %s, ", fileInfo->mGroup);
	printf("Permissions: %d \n", fileInfo->mPermissions);
}

int listDirectory(hdfsFS * fs, const char * directoryName)
{
	if (!*fs)
	{
		fprintf(stderr, "Could not connect to hdfs");
		return -1;
	}

	int * tries = new int(2);
	hdfsFileInfo * fileinf = hdfsListDirectory(*fs, directoryName, tries);
	if (fileinf)
		fprintf(stdout, "%s", fileinf->mName);
	else
		fprintf(stdout, "nofileinf");
	return 0;
}

void getLastXMLElement(string * element, const char * xpath)
{
	int lasttagclosechar = strlen(xpath)-1;
	int lasttagopenchar = 0;

	while(lasttagclosechar >= 0)
	{
		if (xpath[lasttagclosechar] == '>')
			break;
		lasttagclosechar--;
	}
	lasttagopenchar = lasttagclosechar;
	while(lasttagopenchar >= 0)
	{
		if (xpath[lasttagopenchar] == '<')
			break;
		lasttagopenchar--;
	}

	element->append(xpath, lasttagopenchar+1, lasttagclosechar-1);
}

void getLastXPathElement( string * element, const char * xpath)
{
	int lastdelimiter = strlen(xpath)-1;
	while(lastdelimiter >= 0)
	{
		if (xpath[lastdelimiter] == '/')
			break;
		lastdelimiter--;
	}

	element->append(xpath, lastdelimiter+1, strlen(xpath)-lastdelimiter);
}

void getFirstXPathElement(string * element, const char * xpath)
{
	int len = strlen(xpath);
	for(int i = 0; i < len; i++)
	{
		element->append(1, xpath[i]);
		if (xpath[i] == '/')
			break;
	}
}


void xpath2xml(string * xml, const char * xpath, bool open)
{
	vector<string> elements;

	unsigned xpathlen = strlen(xpath);
	for (unsigned i = 0; i < xpathlen;)
	{
		string tmpstr;
		for (; i < strlen(xpath) && xpath[i] != '/';)
			tmpstr.append(1, xpath[i++]);
		i++;
		if (tmpstr.size() > 0)
			elements.push_back(tmpstr);
	}

	if (open)
	{
		for (vector<string>::iterator t = elements.begin();
				t != elements.end() - 1; ++t)
			xml->append(1, '<').append(t->c_str()).append(1, '>');
	}
	else
	{
		vector<string>::reverse_iterator rit;
		for (rit = elements.rbegin() + 1; rit < elements.rend(); ++rit)
			xml->append("</").append(rit->c_str()).append(1, '>');
	}
}

int readXMLOffset(hdfsFS * fs, const char * filename,
		unsigned long seekPos, unsigned long readlen, const char * rowTag, const char * headerText, const char * footerText, unsigned long bufferSize)
{
	string xmlizedxpath;
	string elementname;
	string rootelement;
	//xmlizedxpath.append(headerText);
	xpath2xml(&xmlizedxpath, rowTag, true);
	getLastXPathElement(&elementname, rowTag);


	hdfsFile readFile = hdfsOpenFile(*fs, filename, O_RDONLY, 0, 0, 0);
	if (!readFile)
	{
		fprintf(stderr, "Failed to open %s for reading!\n", filename);
		exit(-1);
	}

	if (hdfsSeek(fs, readFile, seekPos))
	{
		fprintf(stderr, "Failed to seek %s for reading!\n", filename);
		exit(-1);
	}

	//unsigned long bytesAvailable = hdfsAvailable(*fs, readFile);

	//if (bytesAvailable < readlen)
		//readlen = bytesAvailable;

	unsigned char buffer[bufferSize + 1];

	bool firstRowfound = false;

	string openRowTag("<");
	openRowTag.append(elementname).append(1, '>');


	string closeRowTag("</");
	closeRowTag.append(elementname).append(1, '>');


	string closeRootTag("</");
	getLastXMLElement(&closeRootTag, footerText);
	closeRootTag.append(1, '>');

	//unsigned long currentPos = seekPos;
	//TODO --
	unsigned long currentPos = seekPos + openRowTag.size();

	string currentTag("");
	bool withinRecord = false;
	bool stopAtNextClosingTag = false;
	bool parsingTag = false;

	fprintf(stderr, "--Start looking <%s>: %ld--\n", elementname.c_str(), currentPos);

	fprintf(stdout, "%s", xmlizedxpath.c_str());

	unsigned long bytesLeft = readlen;
	while(hdfsAvailable(*fs, readFile) && bytesLeft > 0)
	{
		tSize numOfBytesRead = hdfsRead(*fs, readFile, (void*) buffer,
				bufferSize);
		if (numOfBytesRead <= 0)
		{
			fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
			break;
		}

		for (int buffIndex = 0; buffIndex < numOfBytesRead;)
		{
			char currChar = buffer[buffIndex];

			if (currChar == '<' || parsingTag)
			{
				if (!parsingTag)
					currentTag.clear();

				int tagpos = buffIndex;
				while (tagpos < numOfBytesRead)
				{
					currentTag.append(1, buffer[tagpos++]);
					if (buffer[tagpos - 1] == '>')
						break;
				}

				if (tagpos == numOfBytesRead && buffer[tagpos - 1] != '>')
				{
					fprintf(stderr, "\nTag accross buffer reads...\n");

					currentPos += tagpos - buffIndex;
					bytesLeft -= tagpos - buffIndex;

					buffIndex = tagpos;
					parsingTag = true;

					if (bytesLeft <= 0)
					{
						bytesLeft = readlen; //not sure how much longer til next EOL read up readlen;
						stopAtNextClosingTag = true;
					}
					break;
				}
				else
					parsingTag = false;

				if (!firstRowfound)
				{
					firstRowfound = strcmp(currentTag.c_str(),
							openRowTag.c_str()) == 0;
					if (firstRowfound)
						fprintf(stderr, "--start streaming tag %s at %lu--\n",
								currentTag.c_str(), currentPos);
				}

				if (strcmp(currentTag.c_str(), closeRootTag.c_str()) == 0)
				{
					bytesLeft = 0;
					break;
				}

				if (strcmp(currentTag.c_str(), openRowTag.c_str()) == 0)
					withinRecord = true;
				else if (strcmp(currentTag.c_str(), closeRowTag.c_str()) == 0)
					withinRecord = false;
				else if (firstRowfound && !withinRecord)
				{
					bytesLeft = 0;
					fprintf(stderr,	"Unexpected Tag found: %s at position %lu\n",
							currentTag.c_str(), currentPos);
					break;
				}

				currentPos += tagpos - buffIndex;
				bytesLeft -= tagpos - buffIndex;

				buffIndex = tagpos;

				if (bytesLeft <= 0 && !withinRecord)
					stopAtNextClosingTag = true;


				if (stopAtNextClosingTag
						&& strcmp(currentTag.c_str(), closeRowTag.c_str()) == 0)
				{
					fprintf(stdout, "%s", currentTag.c_str());
					fprintf(stderr, "--stop streaming at %s %lu--\n",
							currentTag.c_str(), currentPos);
					bytesLeft = 0;
					break;
				}

				if (firstRowfound)
					fprintf(stdout, "%s", currentTag.c_str());
				else
					fprintf(stderr, "skipping tag %s\n", currentTag.c_str());

				if (buffIndex < numOfBytesRead)
					currChar = buffer[buffIndex];
				else
					break;
			}

			if (firstRowfound)
			{
				fprintf(stdout, "%c", currChar);
				//bytesLeft--;
			}

			buffIndex++;
			currentPos++;
			//TODO -- not sure about bytesLeft--;
			bytesLeft--;

			if (bytesLeft <= 0)
			{
				if (withinRecord)
				{	fprintf(stderr, "\n--Looking for last closing row tag: %ld--\n",
						currentPos);
					bytesLeft = readlen; //not sure how much longer til next EOL read up readlen;
					stopAtNextClosingTag = true;
				}
				else
					break;
			}
		}
	}

	xmlizedxpath.clear();

	xpath2xml(&xmlizedxpath, rowTag, false);
	fprintf(stdout, "%s", xmlizedxpath.c_str());

	return 0;
}

int readCSVOffset(hdfsFS * fs, const char * filename, unsigned long seekPos,
		unsigned long readlen, const char * eolseq, unsigned long bufferSize, bool outputTerminator,
		unsigned long recLen, unsigned long maxlen, const char * quote)
{
	fprintf(stderr, "CSV terminator: \'%s\' and quote: \'%c\'\n", eolseq, quote[0]);
	unsigned long recsFound = 0;


	hdfsFile readFile = hdfsOpenFile(*fs, filename, O_RDONLY, 0, 0, 0);
	if (!readFile)
	{
		fprintf(stderr, "Failed to open %s for reading!\n", filename);
		exit(-1);
	}

	unsigned eolseqlen = strlen(eolseq);
	if (seekPos > eolseqlen)
		seekPos -= eolseqlen; //read back sizeof(EOL) in case the seekpos happens to be a the first char after an EOL

	if (hdfsSeek(fs, readFile, seekPos))
	{
		fprintf(stderr, "Failed to seek %s for reading!\n", filename);
		exit(-1);
	}

	//unsigned long bytesAvailable = hdfsAvailable(*fs, readFile);

	//if (bytesAvailable < readlen)
		//readlen = bytesAvailable;

	bool withinQuote = false;
	unsigned char buffer[bufferSize + 1];

	bool stopAtNextEOL = false;
	bool firstEOLfound = seekPos == 0 ? true : false;

	unsigned long currentPos = seekPos;

	fprintf(stderr, "--Start looking: %ld--\n", currentPos);

	unsigned long bytesLeft = readlen;
	while(hdfsAvailable(*fs, readFile) && bytesLeft >0)
	{
		tSize num_read_bytes = hdfsRead(*fs, readFile, (void*) buffer, bufferSize);

		if (num_read_bytes <= 0)
		{
			fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
			break;
		}
		for (int bufferIndex = 0; bufferIndex < num_read_bytes; bufferIndex++, currentPos++)
		{
			char currChar = buffer[bufferIndex];

			if (currChar == EOF)
				break;

			if (currChar == quote[0])
			{
				fprintf(stderr, "found quote char at pos: %ld\n", currentPos);
				withinQuote = !withinQuote;
			}

			if (currChar == eolseq[0] && !withinQuote)
			{
				bool eolfound = true;
				tSize extraNumOfBytesRead = 0;
				string tmpstr("");

				if (eolseqlen > 1)
				{
					int eoli = bufferIndex;
					while (eoli < num_read_bytes && eoli - bufferIndex < eolseqlen)
					{
						tmpstr.append(1, buffer[eoli++]);
					}

					if (eoli == num_read_bytes && tmpstr.size() < eolseqlen)
					{
						//looks like we have to do a remote read, but before we do, let's make sure the substring matches
						if (strncmp(eolseq, tmpstr.c_str(), tmpstr.size())==0)
						{
							unsigned char tmpbuffer[eolseqlen - tmpstr.size() + 1];
							//TODO have to make a read... of eolseqlen - tmpstr.size is it worth it?
							extraNumOfBytesRead = hdfsRead(*fs, readFile, (void*) tmpbuffer,
									eolseqlen - tmpstr.size());

							for(int y = 0; y < extraNumOfBytesRead; y++)
								tmpstr.append(1, tmpbuffer[y]);
						}
					}

					if (strcmp(tmpstr.c_str(), eolseq) != 0)
						eolfound = false;
				}

				if (eolfound)
				{
					if (!firstEOLfound)
					{
						bufferIndex = bufferIndex + eolseqlen - 1;
						currentPos = currentPos + eolseqlen - 1;
						bytesLeft = bytesLeft - eolseqlen;

						fprintf(stderr, "\n--Start streaming: %ld--\n", currentPos);

						firstEOLfound = true;
						continue;
					}

					if (outputTerminator)
					{
						//if (currentPos > seekPos) //Don't output first EOL
							fprintf(stdout, "%s", eolseq) ;

						bufferIndex += eolseqlen;
						currentPos += eolseqlen;
						bytesLeft -= eolseqlen;
					}

					recsFound++;
					//fprintf(stderr, "\nrecsfound: %ld", recsFound);
					if (stopAtNextEOL)
					{
						fprintf(stderr, "\n--Stop streaming: %ld--\n", currentPos);
						//fprintf(stdout, "%s", eolseq);
						bytesLeft = 0;
						break;
					}

					if (bufferIndex < num_read_bytes)
						currChar = buffer[bufferIndex];
					else
						break;
				}
				else if(extraNumOfBytesRead > 0)
				{
					if(hdfsSeek(fs, readFile, hdfsTell(fs, readFile)-extraNumOfBytesRead))
					{
							fprintf(stderr, "Error while attempting to correct seek position\n");
							exit(-1);
					}
				}
			}

			//don't stream until we're beyond the first EOL (if offset = 0 start streaming ASAP)
			if (firstEOLfound)
			{
				fprintf(stdout, "%c", currChar);
				bytesLeft--;
			}
			else
			{
				fprintf(stderr, "%c", currChar);
				bytesLeft--;
				if(recLen > 0 && currentPos-seekPos > recLen * 100)
				{
					fprintf(stderr, "\nFirst EOL was not found within the first %lu bytes", currentPos-seekPos);
					exit(-1);
				}
			}

			if (stopAtNextEOL)
				fprintf(stderr, "%c", currChar);

			// ok, so if bytesLeft <= 0 at this point, we need to keep reading
			// IF the last char read was not an EOL char
			if (bytesLeft <= 0	&& currChar != eolseq[0])
			{
				if(!firstEOLfound)
				{
					fprintf(stderr, "\n--Reached end of readlen before finding first record start at: %ld (breaking out)--\n",	currentPos);
					break;
				}

				fprintf(stderr, "\n--Looking for Last EOL: %ld--\n", currentPos);
				bytesLeft = readlen; //not sure how much longer until next EOL read up readlen;
				stopAtNextEOL = true;
			}
		}
	}

	fprintf(stderr, "\nCurrentPos: %ld, RecsFound: %ld\n", currentPos, recsFound);
	hdfsCloseFile(*fs, readFile);

	return 0;
}

int readFileOffset(hdfsFS * fs, const char * filename, tOffset seekPos,
		unsigned long readlen, unsigned long bufferSize)
{
	hdfsFile readFile = hdfsOpenFile(*fs, filename, O_RDONLY, 0, 0, 0);
	if (!readFile)
	{
		fprintf(stderr, "Failed to open %s for reading!\n", filename);
		exit(-1);
	}

	if (hdfsSeek(fs, readFile, seekPos))
	{
		fprintf(stderr, "Failed to seek %s for reading!\n", filename);
		exit(-1);
	}

	//unsigned long bytesAvailable = hdfsAvailable(*fs, readFile);

	//if (bytesAvailable < readlen)
		//readlen = bytesAvailable;

	unsigned char buffer[bufferSize + 1];

	unsigned long currentPos = seekPos;

	fprintf(stderr, "\n--Start streaming: %ld--\n", currentPos);

	unsigned long bytesLeft = readlen;
	while(hdfsAvailable(*fs, readFile) && bytesLeft >0)
	{
		tSize num_read_bytes = hdfsRead(*fs, readFile, buffer,
				bytesLeft < bufferSize ? bytesLeft : bufferSize);
		if (num_read_bytes <= 0)
			break;
		bytesLeft -= num_read_bytes;
		for (int i = 0; i < num_read_bytes; i++, currentPos++)
			fprintf(stdout, "%c", buffer[i]);
	}

	fprintf(stderr, "--\nStop Streaming: %ld--\n", currentPos);

	hdfsCloseFile(*fs, readFile);

	return 0;
}

int streamInFile(hdfsFS * fs, const char * rfile, int bufferSize)
{
	if (!*fs)
	{
		fprintf(stderr, "Could not connect to hdfs on");
		return -1;
	}

	unsigned long fileTotalSize = 0;

	hdfsFileInfo *fileInfo = NULL;
	if ((fileInfo = hdfsGetPathInfo(*fs, rfile)) != NULL)
	{
		fileTotalSize = fileInfo->mSize;
		hdfsFreeFileInfo(fileInfo, 1);
	}
	else
	{
		fprintf(stderr, "Error: hdfsGetPathInfo for %s - FAILED!\n", rfile);
		return -1;
	}

	hdfsFile readFile = hdfsOpenFile(*fs, rfile, O_RDONLY, bufferSize, 0, 0);
	if (!readFile)
	{
		fprintf(stderr, "Failed to open %s for writing!\n", rfile);
		return -2;
	}

	unsigned char buff[bufferSize + 1];
	buff[bufferSize] = '\0';

	for (unsigned long bytes_read = 0; bytes_read < fileTotalSize;)
	{
		unsigned long read_length = hdfsRead(*fs, readFile, buff, bufferSize);
		bytes_read += read_length;
		for (unsigned long i = 0; i < read_length; i++)
			fprintf(stdout, "%c", buff[i]);
	}

	hdfsCloseFile(*fs, readFile);

	return 0;
}

int mergeFile(hdfsFS * fs, const char * filename, unsigned nodeid, unsigned clustercount, unsigned bufferSize, unsigned flushthreshold, short filereplication, bool deleteparts)
{
	if (nodeid == 0)
	{
		fprintf(stderr, "merging %d file(s) into %s", clustercount, filename);
		fprintf(stderr, "Opening %s for writing!\n", filename);

		hdfsFile writeFile = hdfsOpenFile(*fs, filename, O_CREAT | O_WRONLY, 0, filereplication, 0);

		if(!writeFile)
		{
			fprintf(stderr, "Failed to open %s for writing!\n", filename);
			exit(-1);
		}

		tSize totalBytesWritten = 0;
		for (unsigned node = 0; node < clustercount; node++)
		{
			if (node > 0)
			{
				//writeFile = hdfsOpenFile(*fs, filename, O_WRONLY|O_APPEND, 0, 0, 0);
				writeFile = hdfsOpenFile(*fs, filename, O_WRONLY|O_APPEND, 0, filereplication, 0);
				fprintf(stderr, "Re-opening %s for append!\n", filename);
			}

			unsigned bytesWrittenSinceLastFlush = 0;
			char filepartname[1024];
			memset(&filepartname[0], 0, sizeof(filepartname));
			sprintf(filepartname,"%s_%d_%d", filename, node, clustercount);
			if (hdfsExists(*fs, filepartname)==0)
			{

				fprintf(stderr, "Opening readfile  %s\n", filepartname);
				hdfsFile readFile = hdfsOpenFile(*fs, filepartname, O_RDONLY, 0, 0, 0);
				if (!readFile)
				{
					fprintf(stderr, "Failed to open %s for reading!\n", filename);
					exit(-1);
				}

				unsigned char buffer[bufferSize + 1];
				while (hdfsAvailable(*fs, readFile))
				{
					tSize num_read_bytes = hdfsRead(*fs, readFile, buffer, bufferSize);

					if (num_read_bytes <= 0)
						break;

					tSize bytesWritten = 0;
					try
					{
						bytesWritten = hdfsWrite(*fs, writeFile, (void*)buffer, num_read_bytes);
						totalBytesWritten += bytesWritten;
						bytesWrittenSinceLastFlush += bytesWritten;

						if(bytesWrittenSinceLastFlush >= flushthreshold)
						{
							if (hdfsFlush(*fs, writeFile))
							{
								fprintf(stderr, "Failed to 'flush' %s\n", filename);
								exit(-1);
							}
							bytesWrittenSinceLastFlush = 0;
						}
					}
					catch (...)
					{
						fprintf(stderr, "Issue detected during HDFSWrite\n");
						fprintf(stderr, "Bytes written in current iteration: %d\n", bytesWritten);
						exit(-1);
					}
				}
				if (hdfsFlush(*fs, writeFile))
				{
					fprintf(stderr, "Failed to 'flush' %s\n", filename);
					exit(-1);
				}

				fprintf(stderr, "Closing readfile  %s\n", filepartname);
				hdfsCloseFile(*fs, readFile);

				if(deleteparts)
				{
					hdfsDelete(*fs, filepartname);
				}
			}
			else
			{
				fprintf(stderr, "Could not merge, part %s was not located\n", filepartname);
				exit(-1);
			}

			fprintf(stderr, "Closing writefile %s\n", filename);
			hdfsCloseFile(*fs, writeFile);
		}
	}
	return 0;
}

int writeFlatOffset(hdfsFS * fs, const char * filename, unsigned nodeid, unsigned clustercount, const char * fileorpipename)
{

	char filepartname[1024];
	sprintf(filepartname,"%s_%d_%d", filename, nodeid, clustercount);

	//TODO: Consider forcing no replication! should speed up write speed!
	// Should also investigate, if buffer size, and block size should be altered, for
	// performance (currently, allowing local hadoop configuration to dictate those values)
	//hdfsFile writeFile = hdfsOpenFile(*fs, filepartname, O_CREAT | O_WRONLY, 0, 0, 0);
	hdfsFile writeFile = hdfsOpenFile(*fs, filepartname, O_CREAT | O_WRONLY, 0, 1, 0);

	if(!writeFile)
	{
		fprintf(stderr, "Failed to open %s for writing!\n", filepartname);
		exit(-1);
	}

    fprintf(stderr, "Opened HDFS file %s for writing successfully...\n", filepartname);

	fprintf(stderr, "Opening pipe:  %s \n", fileorpipename);

 	ifstream in;
 	in.open(fileorpipename, ios::in|ios::binary);

 	char char_ptr[124*100]; //TODO: this should be configurable.
 							// should it be bigger/smaller?
 							// should it match the HDFS file block size?

	size_t bytesread = 0;
	size_t totalbytesread = 0;
	size_t totalbyteswritten = 0;

	fprintf(stderr, "Writing %s to HDFS [.", filepartname);
 	while(!in.eof())
 	{
 		memset(&char_ptr[0], 0, sizeof(char_ptr));
 		in.read(char_ptr, sizeof(char_ptr));
 		bytesread = in.gcount();
 		totalbytesread += bytesread;
 		tSize num_written_bytes = hdfsWrite(*fs, writeFile, (void*)char_ptr, bytesread);
 		totalbyteswritten += num_written_bytes;

 		fprintf(stderr, ".");
 		//Need to figure out how often this should be done
 		//if(totalbyteswritten % )

 		{
			if (hdfsFlush(*fs, writeFile))
			{
				fprintf(stderr, "Failed to 'flush' %s\n", filepartname);
				exit(-1);
			}
 		}
 	}
 	in.close();

 	if (hdfsFlush(*fs, writeFile))
	{
 		fprintf(stderr, "Failed to 'flush' %s\n", filepartname);
		exit(-1);
	}
 	fprintf(stderr, "]");

	fprintf(stderr,"\n total read: %lu, total written: %lu\n", totalbytesread, totalbyteswritten);

	int clos = hdfsCloseFile(*fs, writeFile);
	fprintf(stderr, "hdfsCloseFile result: %d", clos);

	return 0;
}

void escapedStringToChars(const char * source, string & escaped)
{
	int si = 0;

	while(source[si])
	{
		if (source[si] == '\\')
		{
			switch(source[++si])
			{
			case 'n':
				escaped.append(1, '\n');
				break;
			case 'r':
				escaped.append(1,'\r');
				break;
			case 't':
				escaped.append(1,'\t');
				break;
			case 'b':
				escaped.append(1,'\b');
				break;
			case 'v':
				escaped.append(1,'\v');
				break;
			case 'f':
				escaped.append(1,'\f');
				break;
			case '\\':
				escaped.append(1,'\\');
				break;
			case '\'':
				//fprintf(stderr, "adding escaped single quote..");
				escaped.append(1,'\'');
				break;
			case '\"':
				escaped.append(1,'\"');
				break;
			case '0':
				escaped.append(1,'\0');
				break;
//			case 'c':
//				escaped.append(1,'\c');
//				break;
			case 'a':
				escaped.append(1,'\a');
				break;
//			case 's':
//				escaped.append(1,'\s');
//				break;
			case 'e':
				escaped.append(1,'\e');
				break;
			default:
				break;

			}
		}
		else
			escaped.append(1, source[si]);

		si++;
	}
}

int main(int argc, char **argv)
{
	unsigned int bufferSize = 1024 * 100;
	unsigned int flushThreshold = bufferSize * 10;
	int returnCode = -1;
	unsigned clusterCount = 0;
	unsigned nodeID = 0;
	unsigned long recLen = 0;
	unsigned long maxLen = 0;
	const char * fileName = "";
	const char * hadoopHost = "default";
	int hadoopPort = 0;
	string format("");
	string foptions("");
	string data("");

	int currParam = 1;

	const char * wuid = "";
	const char * rowTag = "Row";
	const char * separator = "";
	//const char * terminator = "";
	string terminator (EOL);
	bool outputTerminator = true;
	//const char * quote = "";
	string quote ("'");
	const char * headerText = "<Dataset>";
	const char * footerText = "</Dataset>";
	const char * hdfsuser = "";
	const char * hdfsgroup = "";
	const char * pipepath = "";
	const char * hdfsgroups[1];
	bool cleanmerge = false;
	short filereplication = 1;

	enum HadoopPluginAction
	{
		HPA_INVALID = -1,
		HPA_STREAMIN = 0,
		HPA_STREAMOUT = 1,
		HPA_STREAMOUTPIPE =2,
		HPA_READOUT = 3,
		HPA_MERGEFILE =4
	};

	HadoopPluginAction action = HPA_INVALID;

	while (currParam < argc)
	{
		if (strcmp(argv[currParam], "-si") == 0)
		{
			action = HPA_STREAMIN;
		}
		else if (strcmp(argv[currParam], "-so") == 0)
		{
			action = HPA_STREAMOUT;
		}
		else if (strcmp(argv[currParam], "-sop") == 0)
		{
			action = HPA_STREAMOUTPIPE;
		}
		else if (strcmp(argv[currParam], "-mf") == 0)
		{
			action = HPA_MERGEFILE;
		}
		else if (strcmp(argv[currParam], "-clustercount") == 0)
		{
			clusterCount = atoi(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-nodeid") == 0)
		{
			nodeID = atoi(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-reclen") == 0)
		{
			recLen = atol(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-format") == 0)
		{
			const char * tmp = argv[++currParam];
			while (*tmp && *tmp != '(')
				format.append(1, *tmp++);
			fprintf(stderr, "Format: %s", format.c_str());
			if (*tmp++)
				while (*tmp && *tmp != ')')
					foptions.append(1, *tmp++);
		}
		else if (strcmp(argv[currParam], "-rowtag") == 0)
		{
			//xmlPath = argv[++currParam];
			rowTag = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-filename") == 0)
		{
			fileName = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-host") == 0)
		{
			hadoopHost = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-port") == 0)
		{
			hadoopPort = atoi(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-wuid") == 0)
		{
			wuid = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-data") == 0)
		{
			data.append(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-separator") == 0)
		{
			separator = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-terminator") == 0)
		{
			terminator.clear();
			escapedStringToChars(argv[++currParam], terminator);
		}
		else if (strcmp(argv[currParam], "-quote") == 0)
		{
			quote.clear();
			escapedStringToChars(argv[++currParam], quote);
		}
		else if (strcmp(argv[currParam], "-headertext") == 0)
		{
			headerText = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-footertext") == 0)
		{
			footerText = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-buffsize") == 0)
		{
			bufferSize = atol(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-outputterminator") == 0)
		{
			outputTerminator = atoi(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-maxlen") == 0)
		{
			//maxLen = atoi(argv[++currParam]);
			maxLen = atol(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-hdfsuser") == 0)
		{
			hdfsuser = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-hdfsgroup") == 0)
		{
			hdfsgroup = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-pipepath") == 0)
		{
			pipepath = argv[++currParam];
		}
		else if (strcmp(argv[currParam], "-flushsize") == 0)
		{
			flushThreshold = atol(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-cleanmerge") == 0)
		{
			cleanmerge = atoi(argv[++currParam]);
		}
		else if (strcmp(argv[currParam], "-hdfsfilereplication") == 0)
		{
			filereplication = atoi(argv[++currParam]);
		}
		else
		{
			fprintf(stderr,
					"Error: Found invalid input param: %s \n", argv[currParam]);
			exit(-1);
		}
		currParam++;
	}

	hdfsFS fs = NULL;
	if (strlen(hdfsuser)>0)
	{
		if (strlen(hdfsgroup)>0)
			hdfsgroups[0] = hdfsgroup;
		else
			hdfsgroups[0] = "supergroup";
		//This was a pre HADOOP 1.0 API call
		//fs = hdfsConnectAsUser(hadoopHost, hadoopPort,hdfsuser,hdfsgroups, 1);
		fs = hdfsConnectAsUser(hadoopHost, hadoopPort,hdfsuser);
	}
	else
		fs = hdfsConnect(hadoopHost, hadoopPort);

	if (!fs)
	{
		fprintf(stderr, "Could not connect to hdfs on %s:%d", hadoopHost,
				hadoopPort);
		exit(-1);
	}

	if (action == HPA_STREAMIN)
	{
		fprintf(stderr, "Streaming in %s...\n", fileName);

		unsigned long fileSize = getFileSize(&fs, fileName);

		if (strcmp(format.c_str(), "FLAT") == 0)
		{
			unsigned long recstoread = getRecordCount(fileSize, clusterCount,
					recLen, nodeID);
			unsigned long offset = nodeID * (fileSize / clusterCount / recLen)
					* recLen;
			if ((fileSize / recLen) % clusterCount > 0)
			{
				if ((fileSize / recLen) % clusterCount > nodeID)
					offset += nodeID * recLen;
				else
					offset += ((fileSize / recLen) % clusterCount) * recLen;
			}

			fprintf(
					stderr,
					"fileSize: %lu offset: %lu size bytes: %lu, recstoread:%lu\n",
					fileSize, offset, recstoread * recLen, recstoread);
			if (offset < fileSize)
				returnCode = readFileOffset(&fs, fileName, offset,
						recstoread * recLen, bufferSize);
		}
		else if (strcmp(format.c_str(), "CSV") == 0)
		{
			fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n",
					fileSize, (fileSize / clusterCount) * nodeID,
					fileSize / clusterCount);

			returnCode = readCSVOffset(&fs, fileName,
					(fileSize / clusterCount) * nodeID, fileSize / clusterCount,
					terminator.c_str(),
					bufferSize,
					outputTerminator,
					recLen,
					maxLen,
					quote.c_str());
		}
		else if (strcmp(format.c_str(), "XML") == 0)
		{
			fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n",
					fileSize, (fileSize / clusterCount) * nodeID,
					fileSize / clusterCount);
			returnCode = readXMLOffset(&fs, fileName,
					(fileSize / clusterCount) * nodeID,
					fileSize / clusterCount,
					rowTag,
					headerText,
					footerText,
					bufferSize);
		}
		else
			fprintf(stderr, "Unknown format type: %s(%s)", format.c_str(),					foptions.c_str());
	}
	else if ( action == HPA_STREAMOUT)
	{
		returnCode = writeFlatOffset(&fs, fileName, nodeID, clusterCount, pipepath	);
	}
	else if ( action == HPA_STREAMOUTPIPE)
	{
		returnCode = writeFlatOffset(&fs, fileName, nodeID, clusterCount, pipepath);
	}
	else if (action == HPA_MERGEFILE)
	{
		returnCode = mergeFile(&fs, fileName, nodeID, clusterCount, bufferSize, flushThreshold, filereplication, cleanmerge);
	}

	int dis = hdfsDisconnect(fs);

	fprintf(stderr, "\nhdfsDisconnect return: %d\n", dis);

	return returnCode;
}
