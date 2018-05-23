#! /bin/sh

WORK_PATH=`dirname $0`
PACKAGE_DIR='package'

CURR_DIR=`pwd`
PROJECT_NAME=`basename $CURR_DIR`

LOG(){
	echo $1 $2 $3 $4 $5 $5 $6 $7 $8 $9
}

LOG "Project: $PROJECT_NAME"


# 1. clean project

LOG "clean project 。。。"

if [ ! -d "$PACKAGE_DIR" ];then
	LOG "create dir $PACKAGE_DIR"	
	mkdir $PACKAGE_DIR
fi

rm -rf $PACKAGE_DIR/*


# 2. build project contents

LOG "build project ... "

mvn install
#mvn dependency:copy-dependencies -DoutputDirectory=./$PACKAGE_DIR/lib

cp ./target/*.jar ./$PACKAGE_DIR
#cp ./mydata4vipweek2.dat ./$PACKAGE_DIR


# remove common jars
rm -rf ./$PACKAGE_DIR/lib/hadoop-*-*.jar
#rm -rf ./$PACKAGE_DIR/lib/hbase-*-*.jar
rm -rf ./$PACKAGE_DIR/lib/spark-*-*.jar
rm -rf ./$PACKAGE_DIR/lib/scala*-*.jar
rm -rf ./$PACKAGE_DIR/lib/servlet-*.jar
mv ./$PACKAGE_DIR/parquet-mergetool-*-SNAPSHOT.jar ./$PACKAGE_DIR/parquet-mergetool.jar

# 3. package

LOG "packaging ..."

PACKAGE_NAME="$PROJECT_NAME.zip"

#tar -zcvf $PACKAGE_NAME $PACKAGE_DIR/*
cd $PACKAGE_DIR
zip -r $PACKAGE_NAME *
#mv $PACKAGE_NAME $PACKAGE_DIR

LOG "package successed!"

exit 0
