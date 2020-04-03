JAVAC=/opt/webcc/java/bin/javac
JAVA=/opt/webcc/java/bin/java

all: DefaultLayerImport ImportObserver

DefaultLayerImport: DefaultLayerImport.java
        $(JAVAC) DefaultLayerImport.java

ImportObserver: ImportObserver.java
        $(JAVAC) -Xlint:deprecation ImportObserver.java