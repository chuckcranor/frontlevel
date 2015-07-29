# frontlevel
frontlevel code

to compile: cd src && make               (use GNU make for all)

to do basic tests: cd src && make check

to do dbtests:
 - currently needs a leveldb backend compiled in ../leveldb
 - expects frontlevel src to be compiled
 - see Makefile for directory settings (FLDIR, LDBDIR)
 - compile with GNU make, then run binary
 
