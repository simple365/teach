--脚本，先假定 hdfs dfs -mkdir -p /tmp/teach/logs;

./flume-ng agent --conf ../conf -f ../conf/log-kafka.properties --name a1 -Dflume.root.logger=INFO,file &

kafka 读取数据






------------ sql 相关



create schema teach;
use teach;

-- 原始日志表

drop table if exists stage_originlog_lzo_dt;
CREATE EXTERNAL TABLE `teach`.`stage_originlog_lzo_dt`(`line` string)
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/tmp/teach/logs/';


-- 基础表
drop table if exists ods_basedata_dt;
CREATE EXTERNAL TABLE `ods_basedata_dt`(`user_id` string, `area` string, `app_version` string, `app_time` string, `event_name` string, `event_json` string, `server_time` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/ods_basedata_dt/';



-- 前台后台活跃表
drop table if exists ods_foreground_dt;
CREATE EXTERNAL TABLE `ods_foreground_dt`(`user_id` string, `area` string, `app_version` string, `app_time` string, `create_time` string, `server_time` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/ods_foreground_dt/';


drop table if exists ods_background_dt;
CREATE EXTERNAL TABLE `ods_background_dt`(`user_id` string, `area` string, `app_version` string, `app_time` string, `create_time` string, `server_time` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/ods_background_dt/';


-- 新闻展示表
drop table if exists ods_display_dt;
CREATE EXTERNAL TABLE `ods_display_dt`(`user_id` string, `area` string, `app_version` string, `app_time` string, `action` string,news_id string, `server_time` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/ods_display_dt/';



-- 新闻每日表
drop table if exists mid_daily_news_dt;
CREATE EXTERNAL TABLE `mid_daily_news_dt`(`news_id` string, `area` string,total_display string,total_click string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_daily_news_dt/';

-- 新闻历史表
drop table if exists mid_news_history_dt;
CREATE EXTERNAL TABLE `mid_news_history_dt`(`news_id` string, `area` string, `first_display_time` string,`last_display_time` string,`first_click_time` string,`last_click_time` string,total_display int,total_click int)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_news_history_dt/';

--需要找出新闻最大，最小，自动忽略null值
create temp view tmp_today_news as 
 select t1.*,t2.total_click,t2.total_display from 
 (
select news_id,area,min(first_display_time) first_display_time, max(last_display_time) last_display_time, min(first_click_time) first_click_time, max(last_click_time) last_click_time from (
select news_id,area,min(server_time) first_display_time,max(server_time) last_display_time,null first_click_time,null last_click_time from ods_display_dt where dt='2018-09-30' and action='1' group by news_id,area  
union 
select news_id,area,null first_display_time,null last_display_time,min(server_time) first_click_time,max(server_time) last_click_time from ods_display_dt where dt='2018-09-30' and action='2' group by news_id,area 
)
group by news_id,area 
) t1 join mid_daily_news_dt t2 
on t1.news_id =t2.news_id;
 
insert overwrite table  mid_news_history_dt
partition (dt='2018-09-30')
select 
news_id,area,
min(first_display_time) first_display_time, 
max(last_display_time) last_display_time, 
min(first_click_time) first_click_time, 
max(last_click_time) last_click_time,
sum(total_display) total_display,
sum(total_click) total_click 
from 
(
select * from tmp_today_news 
union all 
select news_id,area,first_display_time,last_display_time,first_click_time,last_click_time,total_display,total_click from mid_news_history_dt where dt='2018-09-29'
) group by news_id,area;
 


--app用户历史表
drop table if exists mid_user_history_dt;
CREATE EXTERNAL TABLE `mid_user_history_dt`(`user_id` string, `area` string, app_version string,`first_dat` string,`last_dat` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_user_history_dt/';

insert overwrite table mid_user_history_dt 
partition (dt='2018-09-30')
select user_id,area,max(app_version) app_version,min(first_dat) first_dat,max(last_dat) last_dat from (
select distinct user_id,area,dt first_dat,dt last_dat from ods_basedata_dt where dt='2018-09-30' 
union
select user_id,area,app_version,first_dat,last_dat from mid_user_history_dt where dt='2018-09-29' 
)group by user_id,area;

select count(distinct user_id) num,app_version from mid_user_history_dt where dt='2018-09-30' group by app_version order by app_version 

--活跃用户表
drop table if exists mid_active_user_dt;
CREATE EXTERNAL TABLE `mid_active_user_dt`(`user_id` string, `area` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_active_user_dt/';

-- 7天内产生点击的用户
insert overwrite table mid_active_user_dt
partition (dt='2018-09-30')
select distinct user_id,area from  ods_display_dt where action='2' and dt>=date_add(current_date,-7);


---- 执行语句



add jar /home/hadoop/teach.jar;
create temporary function base_analizer as 'com.test.BaseFieldUDF';
create temporary function flat_analizer as 'com.test.EventnameUDTF';
set hive.exec.dynamic.partition.mode=nonstrict;

use teach;

alter table stage_originlog_lzo_dt add if not exists partition (dt='2018-09-30') location '/tmp/teach/logs/2018-09-30';

insert overwrite table ods_basedata_dt
PARTITION (dt)
select
   user_id                      ,
   area          ,
   app_version               ,
   app_time                      ,
   event_name ,
   event_json ,
   server_time               ,
   sdk_log.dt
 from
(
select
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[0]   as user_id                  ,
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[1]   as area      ,
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[2]   as app_version           ,
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[3]   as app_time                  ,
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[4]   as server_time                ,
split(base_analizer(line,'userId,area,appVersion,appTime'),'\t')[5]   as ops            ,
dt
from stage_originlog_lzo_dt where dt='2018-09-30'
) sdk_log lateral view flat_analizer(ops) tmp_k as event_name, event_json;


insert overwrite table ods_display_dt
PARTITION (dt)
select
   user_id                      ,
   area          ,
   app_version               ,
   app_time                      ,
   action,
   news_id,
   server_time               ,
   sdk_log.dt
 from
(
select
user_id,
area,
app_version,
app_time,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.newsId') news_id,
server_time,
dt
from ods_basedata_dt where dt='2018-09-30' and event_name='display'
) sdk_log;


insert overwrite table ods_foreground_dt
PARTITION (dt)
select
   user_id                      ,
   area          ,
   app_version               ,
   app_time                      ,
   create_time,
   server_time               ,
   sdk_log.dt
 from
(
select
user_id                  ,
area      ,
app_version           ,
app_time,
get_json_object(event_json,'$.kv.createTime') create_time,
server_time,
dt
from ods_basedata_dt where dt='2018-09-30' and event_name='foreground'
) sdk_log;


insert overwrite table ods_background_dt
PARTITION (dt)
select
   user_id                      ,
   area          ,
   app_version               ,
   app_time                      ,
   create_time,
   server_time               ,
   sdk_log.dt
 from
(
select
user_id                  ,
area      ,
app_version           ,
app_time,
get_json_object(event_json,'$.kv.createTime') create_time,
server_time,
dt
from ods_basedata_dt where dt='2018-09-30' and event_name='background'
) sdk_log;


-- 计算 每天的新闻统计
insert overwrite table mid_daily_news_dt
partition(dt='2018-09-30')
select news_id,area,sum(display) total_display,sum(click) total_click from 
(
select 
case when t1.news_id is not null then t1.news_id else t2.news_id end news_id,
case when t1.area is not null then t1.area else t2.area end area,
case when t1.display is not null then t1.display else 0 end display,
case when t2.click is not null then t2.click else 0 end click 
 from 
(select news_id,area,count(distinct user_id) display from ods_display_dt where dt='2018-09-30' and action='1' group by news_id,area) t1 
full join (select news_id,area,count(distinct user_id) click from ods_display_dt where dt='2018-09-30' and action='2' group by news_id,area) t2
on t1.area=t2.area and t1.news_id=t2.news_id
)group by news_id,area;


---- mongodb 
/data/softwares/mongodb-linux-x86_64-amazon-4.0.2/bin/mongod -f /data/data/teach/mongodb.conf --bind_ip_all
mongod -shutdown

db.createUser(
    {
      user: "iron",
      pwd: "man",
      roles: [
         { role: "readWrite", db: "test" }  
      ]
    }
)

mongo -u iron -p man -host 10.86.199.238 test

display()

--- mysql
drop table if exists news_display;
create table news_display(
area varchar(20),
news_id varchar(20),
user_id varchar(20),
time_stam long
);
alter table news_display add unique(area,news_id,user_id);

drop table if exists news_click;
create table news_click(
area varchar(20),
news_id varchar(20),
user_id varchar(20),
time_stam long
);
alter table news_click add unique(area,news_id,user_id);

drop table if exists tmp_display_click;
create table tmp_display_click(
action int(2),
area varchar(20),
news_id varchar(20),
user_id varchar(20),
time_stam long
);
alter table tmp_display_click add index(action,area,news_id,user_id);
 
delete t1 from news_display t1 join (select area,news_id,user_id,time_stam from tmp_display_click where action=1) t2 
on t1.area=t2.area and t1.news_id=t2.news_id and t1.user_id=t2.user_id;

insert ignore into news_display select area,news_id,user_id,time_stam from tmp_display_click where action=1;


delete t1 from news_click t1 join (select area,news_id,user_id,time_stam from tmp_display_click where action=2) t2 
on t1.area=t2.area and t1.news_id=t2.news_id and t1.user_id=t2.user_id;

insert ignore into news_click select area,news_id,user_id,time_stam from tmp_display_click where action=2;
select t1.*,
case when t2.click is not null then t2.click else 0 end click  from 
(select count(distinct user_id) display,news_id,area from news_display group by area,news_id) t1 
left join 
(select count(distinct user_id) click ,news_id,area from news_click group by area,news_id) t2  
on t1.news_id = t2.news_id;  


