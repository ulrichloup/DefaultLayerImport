JAVAC=/opt/webcc/java/bin/javac
JAVA=/opt/webcc/java/bin/java

all: PolaraLayerImport ImportObserver

PolaraLayerImport: PolaraLayerImport.java
        $(JAVAC) PolaraLayerImport.java

ImportObserver: ImportObserver.java
        $(JAVAC) -Xlint:deprecation ImportObserver.java