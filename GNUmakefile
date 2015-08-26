Default: all


TOP=$(realpath .)

include $(TOP)/common.mk

CLASSPATH="$(CLASSLIB)$(CLN)$(LIB)$(CLN)$(SRC)"

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
