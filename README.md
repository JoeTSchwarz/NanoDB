# NanoDB
Object Database in Nutshell
Object Database in Brief
This NanoDB package is a small, 100% pure JAVA object-oriented database for JAVA objects (serialized or non-serialized like String). The package consists of
4 APIs, 2 SWING examples and an example SWING NanoDBServer.
- NanoDB: the interface to physical media using RandomAccessFile technology.
- NanoDBConnect: the interface for the client app.
- NanoDBWorker: the interface to NanoDBConnect on the server side.
- NanoDBManager: this API manages and monitors all NanoDB activities in a multi-user environment.

Two examples:
- NanoDBdirect shows you how to work directly with the NanoDB API
- NanoDBClient is derived from NanoDBdirect and shows you how to apply NanoDBConnect API to your application.

SWING NanoDBServer (with SysMonSWING) is an example that shows you how to create your own NanoDB server.

How to run the examples:
- Start NanoDBServer and set the required parameters (hostname/IP, port, path for NanoDB files, max. cache limit).
- With NanoDB "People": First run "CreatePeopleNanoDB" to create a NanpDB file (as a database). This small app reads the text file "people.txt" and generates from it the corresponding Java serialized objects for the NanoDB database.
- You can create a new NanoDB by skipping the input with any name instead of "People" and then either run setAutoCommit before populating the database with addObject OR go the traditional way: addObject-commit-unlock, addObject-commit-unlock, ... and finally close it.

Joe
