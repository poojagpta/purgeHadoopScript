#!/bin/bash
usage="Usage: ./purge-hdfs-file.sh [table_name] [days]"

if [ ! "$1" ]
then
  echo $usage;
  exit 1;
fi

if [ ! "$2" ]
then
  echo $usage;
  exit 1;
fi


now=$(date +%s);
today=`date '+%Y_%m_%d__%H_%M_%S'`;
FILE="./hivescript_$today.ddl";

echo "use ai_semantic_app;" >> $FILE
table_struct=$( hive -S -e "USE ai_semantic_app; desc formatted $1");
hdfs_file_path=`expr match "$table_struct" '.*\(hdfs://[A-Za-z0-9_/]*\)'`;

count_char=`echo $hdfs_file_path | awk -F/ '{print  NF-1}'`;
arg_count=`expr $count_char + 2`;
pri_count=`expr $count_char + 3`;

 if [ ! -z $hdfs_file_path ]; then
   # Loop through files
   hadoop dfs -ls -R $hdfs_file_path| grep "^d"| while read f; do
   # Get File Date and File Name
   file_date=`echo $f | awk '{print $6}'`;
   file_name=`echo $f | awk '{print $8}'`;

   # Calculate Days Difference
   difference=$(( ($now - $(date -d "$file_date" +%s)) / (24 * 60 * 60) ));
   if [ $difference -gt $2 ]; then
     arg_key=`echo $file_name | awk -v var=$arg_count  -F/ '{print $var}'|cut -d'=' -f1`;
     arg_val=`echo $file_name | awk -v var=$arg_count -F/ '{print $var}'|cut -d'=' -f2`;
     pri=`echo $file_name | awk -v var=$pri_count -F/ '{print $var}'`;

      if [ ! -z $arg_key ]  && [ ! -z $arg_val ]  && [ ! -z $pri ];  then
       echo "alter table $1  drop partition($arg_key='$arg_val',$pri);" >> $FILE;
       echo "dfs  -rm -R $file_name ;" >> $FILE;
       #my_value=$( hive -S -e "USE ai_semantic_app;  alter table $1 drop partition($arg_key='$arg_val',$pri); dfs  -rm -R $file_name ; ");
       #echo $my_value
      fi
   fi
  done
 fi
