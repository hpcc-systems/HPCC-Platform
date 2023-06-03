#!/bin/bash

output_file="CMake_Error_Log.txt"

# Clean the output file first
echo "" > $output_file

# specify the directory
directory="./.github/workflows"

# loop over each file in directory
for input_file in $directory/*.log; do

  echo "File: $input_file" >> $output_file
  echo "" >> $output_file
  echo "Error Message:" >> $output_file
  echo "" >> $output_file

  awk '/CMake Error/{flag=1}/^$/{flag=0}flag' $input_file >> $output_file

  # Load error message into a variable
  error_message=$(awk '/CMake Error/{flag=1}/^$/{flag=0}flag' $input_file)
  
  if [[ -z "$error_message" ]]; then
    echo "No CMake errors detected." >> $output_file
    continue
  fi

  echo "" >> $output_file
  echo "Caused by:" >> $output_file
  echo "----------" >> $output_file

# Check error message for known issues
# Check error message for known issues
if [[ $error_message == *"add_subdirectory"* ]]; then
  echo "  - Wrong source branch selected in HPCC-Platform local repo" >> $output_file
  echo "  - GitHub plays funny (rarely it happens, it checks out a version/tag, but not fully and reported ok)" >> $output_file
elif [[ $error_message == *"VERSION"* && $error_message == *"format invalid"* ]]; then
  echo "  - Invalid version format" >> $output_file
elif [[ $error_message == *"Failed to download"* ]]; then
  echo "  - Lack of GH resource" >> $output_file 
fi

echo "" >> $output_file
echo "Handling:" >> $output_file
echo "---------" >> $output_file
if [[ $error_message == *"add_subdirectory"* ]]; then
  echo "  - Capture cmake command output (stdout and stderr)" >> $output_file
  echo "  - It is worth to try clone the repo once again" >> $output_file
  echo "    - The old cloned repo should be removed before the new attempt" >> $output_file
  echo "  - If the error persists, archive:" >> $output_file
  echo "    - Captured cmake output" >> $output_file
  echo "    - HPCC-Platform-build/CMakeFiles/CMakeOutput.log" >> $output_file
  echo "    - HPCC-Platform-build/CMakeFiles/CMakeError.log" >> $output_file
elif [[ $error_message == *"VERSION"* && $error_message == *"format invalid"* ]]; then
  echo "  - Capture cmake command output (stdout and stderr)" >> $output_file
  echo "  - Nothing can do" >> $output_file
  echo "  - Should arhive:" >> $output_file
  echo "    - Captured cmake output" >> $output_file
  echo "    - HPCC-Platform-build/CMakeFiles/CMakeOutput.log" >> $output_file
  echo "    - HPCC-Platform-build/CMakeFiles/CMakeError.log" >> $output_file
elif [[ $error_message == *"Failed to download"* ]]; then
  echo "  - Capture" >> $output_file
  echo "sudo apt-get install -y bison flex build-essential binutils-dev curl lsb-release libcppunit-dev python3-dev default-jdk r-base-dev r-cran-rcpp r-cran-rinside r-cran-inline pkg-config libtool autotools-dev automake git cmake" >> $output_file
  echo "    command output" >> $output_file
  echo "  - It is worth to try rerun cmake a couple of times with some delay between" >> $output_file
  echo "- If the error persists should arhive: Captured apt-get otput" >> $output_file
fi

done
