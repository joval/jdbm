# Copyright (C) 2015 jOVAL.org.  All rights reserved.
# This software is licensed under the JDBM license. See LICENSE.txt in this project.

Default: all

ifeq (x, x$(JAVA_HOME))
    $(error "Please set the JAVA_HOME environment variable.")
endif
PLATFORM=unknown
ifeq (Windows, $(findstring Windows,$(OS)))
  PLATFORM=win
  CLN=;
else
  OS=$(shell uname)
  ifeq (Linux, $(findstring Linux,$(OS)))
    PLATFORM=linux
  endif
  CLN=:
endif
ifeq (win, $(PLATFORM))
    WIN_JAVA_HOME=$(shell cygpath -u $(JAVA_HOME))
    JAVA=$(WIN_JAVA_HOME)/bin/java.exe
    JAR=$(WIN_JAVA_HOME)/bin/jar.exe
    JAVADOC=$(WIN_JAVA_HOME)/bin/javadoc.exe
    JAVAC=$(WIN_JAVA_HOME)/bin/javac.exe
    CLASSLIB=$(shell cygpath -w $(JAVA_HOME))\jre\lib\rt.jar
else
    JAVA=$(JAVA_HOME)/bin/java
    JAR=$(JAVA_HOME)/bin/jar
    JAVADOC=$(JAVA_HOME)/bin/javadoc
    JAVAC=$(JAVA_HOME)/bin/javac
    CLASSLIB=$(JAVA_HOME)/jre/lib/rt.jar
endif
RAW_JAVA_VERSION:=$(shell $(JAVA_HOME)/bin/java -version 2>&1)
ifeq (1.8, $(findstring 1.8, $(RAW_JAVA_VERSION)))
    JAVA_VERSION=1.8
else ifeq (1.7, $(findstring 1.7, $(RAW_JAVA_VERSION)))
    JAVA_VERSION=1.7
else ifeq (1.6, $(findstring 1.6, $(RAW_JAVA_VERSION)))
    JAVA_VERSION=1.6
else
    $(error "Unsupported Java version: $(RAW_JAVA_VERSION)")
endif

NULL:=
SPACE:=$(NULL) # end of the line
SHELL=/bin/sh
CWD=$(shell pwd)

BUILD=build
RSRC=rsrc
DOCS=docs
SRC=src/main
