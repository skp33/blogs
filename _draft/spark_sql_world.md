CREATE TEMPORARY TABLE account USING org.elasticsearch.spark.sql OPTIONS (resource 'bank/account')

CREATE temporary TABLE message USING org.apache.spark.sql.jdbc OPTIONS ( url "jdbc:mysql://192.168.1.16:3306/SMB?user=lalit&password=lalit#321", dbtable "message" );

create temporary table temp_kmeans_data as select a,b from kmeans_data;

cache table people;

create temporary table json using org.apache.spark.sql.json options (path 'hdfs://192.168.1.17:8020/spark_test/data/mllib/output/kmeans_data.json');

CREATE TEMPORARY TABLE avro USING com.databricks.spark.avro OPTIONS (path "hdfs://192.168.1.17:8020/spark_test/data/mllib/output/kmeans_data.avro");

create temporary table parquet using org.apache.spark.sql.parquet options (path 'hdfs://192.168.1.17:8020/spark_test/data/mllib/output/kmeans_data.parquet');

CREATE TEMPORARY TABLE sourcedata USING org.apache.spark.sql.cassandra OPTIONS (table 'sourcedata', keyspace 'smb', cluster 'BDI Cassandra', pushdown 'true');

create table bhp (co int) as select count(*) from sourcedata where admin='mahendra' and searchterm in ('Maruti','Mahindra','TataMotors');

insert overwrite table usautomobiles select * from sourcedata where admin='mahendra' and searchterm in ('ford','gmc','generlmotors','honda','nissan','Oncor','tatamotors','tesla');

/*create temporary table tmpable using org.apache.spark.sql.cassandra options (c_table 'mentionentitydaywise, keyspace 'smb', cluster 'cluster', push_down 'true', spark_cassandra_input_page_row_size '10', spark_cassandra_output_consistency_level 'ONE', spark_cassandra_connection_timeout_ms '1000' );*/


<----------- start log --------------->
create external table access_logs (ipAddress String, clientIdentd String, userid string, dateTimeString String, method String, endpoint String, protocol String, responseCode int, contentSize int) row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' with serdeproperties ('input.regex' = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)') location 'hdfs://192.168.1.17:8020/data/access_logs/';

cache table access_logs;

select ipaddress,datetimestring,method,responsecode,contentsize,endpoint from access_logs limit 5;

+---------------+-----------------------------+---------+---------------+--------------+---------------------------------------+
|   ipaddress   |       datetimestring        | method  | responsecode  | contentsize  |                          endpoint     |
+---------------+-----------------------------+---------+---------------+--------------+---------------------------------------+
| 192.168.1.17  | 29/Jan/2015:11:21:33 +0530  | POST    | 200           | 507          | /BizVizEP/services/rest/bizvizsession |
| 192.168.1.17  | 29/Jan/2015:11:21:37 +0530  | POST    | 200           | 263          | /BizVizEP/services/rest/SpaceManager/ |
| 192.168.1.17  | 29/Jan/2015:11:21:37 +0530  | POST    | 200           | 5564         | /BizVizEP/services/rest/AdminManager/ |
| 192.168.1.17  | 29/Jan/2015:11:21:38 +0530  | POST    | 200           | 550          | /BizVizEP/services/rest/bizvizsession |
| 192.168.1.17  | 29/Jan/2015:11:21:38 +0530  | POST    | 200           | 442          | /BizVizEP/services/rest/DashboardMana |
+---------------+-----------------------------+---------+---------------+--------------+---------------------------------------+


select count(endpoint) endpoint_count,ipaddress, endpoint from access_logs group by endpoint,ipaddress order by endpoint_count desc limit 5;
select distinct ipaddress from access_logs;
select count(ipaddress) ipaddress_count,ipaddress,responsecode from access_logs group by ipaddress,responsecode order by ipaddress_count desc limit 32;

<----------- end log --------------->

##################################################

<----------- start json --------------->

create temporary table tweets using org.apache.spark.sql.json options (path 'hdfs://192.168.1.17:8020/data/json/obama_tweets_2014_09_11_18_23.json');

select unix_timestamp(created_at,"EEE MMM d HH:mm:ss Z yyyy"),text from tweets limit 7;

select from_unixtime(unix_timestamp(created_at,"EEE MMM d HH:mm:ss Z yyyy"), 'yyyy-MM-dd') as dt,text from tweets limit 7;

select top.dt,top.hashtag, count(*) count from (select from_unixtime(unix_timestamp(created_at,"EEE MMM d HH:mm:ss Z yyyy"), 'yyyy-MM-dd') as dt, lower(hashtag) as hashtag from tweets lateral view explode(split(text, ' ')) b as hashtag where created_at is not null and hashtag rlike "^#[a-zA-Z0-9]+$") top group by top.dt,top.hashtag order by count desc limit 10;

select from_unixtime(unix_timestamp(created_at,"EEE MMM d HH:mm:ss Z yyyy"), 'yyyy-MM-dd') as dt, lower(hashtag) as hashtag from tweets lateral view explode(split(text, ' ')) b as hashtag where created_at is not null and hashtag rlike "^#[a-zA-Z0-9]+$";

select dt,hashtag,count(*) count from positive_hashtags_per_day group by dt,hashtag order by count desc limit 9;

select from_unixtime(unix_timestamp(created_at,"EEE MMM d HH:mm:ss Z yyyy"), 'yyyy-MM-dd') as dt, lower(hashtag) as hashtag from tweets lateral view explode(split(text, ' ')) b as hashtag where created_at is not null and hashtag rlike "^#[a-zA-Z0-9]+$" and text rlike "^.*[\;:]-?\$$.*$" limit 6;

<----------- end json --------------->

<----------- start xml --------------->

<xml>
<log>
<logentry
   revision="23939">
<author>mukthi.nath</author>
<date>2015-12-02T06:42:53.866644Z</date>
<paths>
<path
   action="M"
   prop-mods="false"
   text-mods="true"
   kind="file">/Portal/dev/com.bdbizviz.ep/WebContent/events/admin/bizviz-adminmgmt.js</path>
</paths>
<msg>(add #13155) UI of Scheduler</msg>
</logentry>
</log>
</xml>


cat bizviz_svnxmllog.xml | tr -d '&' | tr '\n' ' ' | tr '\r' ' ' | sed 's|</logentry>|</logentry>\n|g' | sed 's/<log>//g' | grep -v '^\s*$'  | sed '3d' > bizviz_svn.xml


use svn_logs;

##### Table for svn_access_logs
create external table svn_access_logs (ipAddress String, datasize int, duration int, filename String, protocol String, method String, responseCode int, urlPath String, connectionStatus String, time String, username String, project String, repo String, svnAction String, svnActionInfo String) row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' with serdeproperties ('input.regex' = '^(\\S+) (\\d+) (\\d+) (\\S+) (\\S+) (\\S+) (\\d{3}) (\\S+) (\\S{1}) (\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (\\S+) (\\S+) (\\S+) (\\S+) (.{0,})');

##### Table for svn access logs in Parquet format
create external table svn_access_logs_parquet(ipAddress String, datasize int, duration int, filename String, protocol String, method String, responseCode int, urlPath String, connectionStatus String, time String, username String, project String, repo String, svnAction String, svnActionInfo String) STORED AS PARQUET;

insert overwrite table svn_access_logs_parquet select * from svn_access_logs;

##### Raw Table for svn xml logs 
Create TABLE svn_xml_logs(xmldata string) STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs://192.168.1.17:8020/data/xml/bizviz_svn.xml' OVERWRITE INTO TABLE svn_xml_logs;

##### View from Raw svn xml logs
drop view svnxmlview;
CREATE VIEW svnxmlview(revision,author, date, msg,kinds,propmods,actions,textmods,paths) AS
SELECT
 xpath_int(xmldata, 'logentry/@revision'),
 xpath_string(xmldata, 'logentry/author/text()'),
 xpath_string(xmldata, 'logentry/date/text()'),
 xpath_string(xmldata, 'logentry/msg/text()'),
xpath(xmldata, 'logentry/paths/path/@kind'),
xpath(xmldata, 'logentry/paths/path/@prop-mods'),
xpath(xmldata, 'logentry/paths/path/@action'),
xpath(xmldata, 'logentry/paths/path/@text-mods'),
xpath(xmldata, 'logentry/paths/path/text()')
FROM svn_xml_logs;

##### Table for svn xml logs 
drop table svn_logs;
create table svn_logs as select revision, author, minute(from_unixtime(unix_timestamp(substr(date,0,23),"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd HH:mm:ss")) ,regexp_extract(msg,"([A-Za-z]+) #([0-9]+)",1) as committype, regexp_extract(msg,"([A-Za-z]+) #([0-9]+)",2) as issueid, trim(regexp_replace(msg,"[(][A-Za-z]+ #[0-9]+[)]", "")) as message, kinds, propmods, actions, textmods, paths from svnxmlview;

##### Table for svn xml logs in Parquet format
create table svn_logs_parquet(revision int,author string, date string,committype string,issueid string, massage string,kinds array<string>,propmods array<string>,actions array<string>,textmods array<string>,paths array<string>) STORED AS PARQUET;

insert overwrite table svn_logs_parquet select revision, author, from_unixtime(unix_timestamp(substr(date,0,23),"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd HH:mm:ss") as date ,regexp_extract(msg,"([A-Za-z]+)\\s#([0-9]+)",1) as committype, regexp_extract(msg,"([A-Za-z]+)\\s#([0-9]+)",2) as issueid, trim(regexp_replace(msg,"[(][A-Za-z]+\\s#[0-9]+[)]", "")) as message, kinds, propmods, actions, textmods, paths from svnxmlview;






