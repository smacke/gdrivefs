NAME := gdrivefs

default: gdrivefs.jar

package: gdrivefs.jar
	./package.sh 0.1

gdrivefs.jar: FORCE
	ant -f packaging/package.xml

FORCE:
