Default: all

TOP=../../..

ifndef JAVA_HOME
    JAVA_HOME=$(TOP)/tools/jdk1.6.0_26
endif

ifeq (Windows, $(findstring Windows,$(OS)))
    CLN=;
else
    CLN=:
endif

NULL:=
SPACE:=$(NULL) # end of the line
SHELL=/bin/sh

JAVA=$(JAVA_HOME)/bin/java
JAVAC=$(JAVA_HOME)/bin/javac
JAR=$(JAVA_HOME)/bin/jar
JAVACFLAGS=-Xlint:unchecked -deprecation
CLASSLIB=$(JAVA_HOME)/jre/lib/rt.jar
RSRC=rsrc
LIBDIR=$(RSRC)/lib
LIB=$(subst $(SPACE),$(CLN),$(filter %.jar %.zip, $(wildcard $(LIBDIR)/*)))
BUILD=build
SRC=src/main
DOCS=docs
CLASSPATH="$(CLASSLIB)$(CLN)$(LIB)$(CLN)$(SRC)"
CWD=$(shell pwd)

include classes.mk

CLASS_FILES:=$(foreach class, $(CLASSES), $(BUILD)/$(subst .,/,$(class)).class)
PACKAGES=$(sort $(basename $(CLASSES)))
PACKAGEDIRS=$(subst .,/,$(PACKAGES))

all: jdbm.jar

jdbm.jar: classes
	$(JAR) cvf $@ -C $(BUILD)/ .

javadocs:
	mkdir -p $(DOCS)
	$(JAVA_HOME)/bin/javadoc -d $(DOCS) -classpath $(CLASSPATH) $(PACKAGES)

clean:
	rm -rf $(BUILD)

install: jdbm.jar
	cp $< $(TOP)/repositories/jSAF/components/provider/rsrc/lib

classes: classdirs $(CLASS_FILES)

classdirs: $(foreach pkg, $(PACKAGEDIRS), $(BUILD)/$(pkg)/)

$(BUILD)/%.class: $(SRC)/%.java
	$(JAVAC) $(JAVACFLAGS) -d $(BUILD) -classpath $(CLASSPATH) $<

$(BUILD)/%/:
	mkdir -p $(subst PKG,,$@)
