Wrench
======

A simple distributed database similar to Google's Spanner

This applications maintains 2 related files (GRADES.TXT and STATS.TXT) 
which are hosted on different machines in sync with each other. Each
file is replicated on multiple servers for high availability. Each 
replica of the file is also kept in sync with the others.

The aim of this application is to demonstrate the use of well-known
distributed computing techniques such as Paxos, 2-phase commit and
leader election.

Team Members
============
* Hiranya Jayathilaka
* Micheal Agun


