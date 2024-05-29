# Developer README for ESP API Command Line Tool

## Overview

This tool is designed to interact with HPCC ESP services, providing four commands: `describe`, `test`, `list-services`, and `list-methods`.

- **`describe`:**
  Allows users to explore available services, methods, and their request-response structures.

- **`test`:**
  Enables sending requests in various formats (XML, JSON, or form query strings) to the ESP services.

- **`list-services`:**
  Invoked by the auto-complete script, this command provides a list of names of all ESP services.

- **`list-methods`:**
  Invoked by the auto-complete script, this command provides a list of names of all methods within an ESP service.

## Usage Notes

- **ESDL Directory Location:**
  The tool gathers the directory of the ESDL files from an environment configuration variable; gets the install path from jutil library and appends /componentfiles/esdl_files/.

- **Server and Port Defaults:**
  When using the `test` command, if the server and port are not specified, the tool defaults to interacting with an ESP instance located at `http://127.0.0.1:8010`.

## Ideas for Expansion

- **Custom ESDL Directory Argument:**
  An additional argument could be introduced to allow users to specify the directory of the ESDL files directly in the command.

- **Template Request Generation:**
  A feature could be added to generate template XML or JSON requests. This would simplify the process of filling out requests by providing a pre-structured template.

- **Credential Prompts:**
  The tool could be expanded to prompt for the username and password upon a 401 Unauthorized response.

- **Selective Response Extraction:**
  Another potential feature is to allow extraction of specific tags from the response using XPath expressions. This would make it easier to parse and analyze responses.
