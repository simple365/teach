------------ sql 相关


create schema dw_weather;
use dw_weather;

-- 原始日志表

drop table if exists stage_originlog_lzo_dt;
CREATE EXTERNAL TABLE `stage_originlog_lzo_dt`(`line` string)
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/tmp/flume/weather';

alter table stage_originlog_lzo_dt add partition(dt='2018-12-12') location '/tmp/flume/weather/2018-12-12';


-- 基础表
drop table if exists ods_basedata_dt;
CREATE EXTERNAL TABLE `ods_basedata_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string, `event_name` string, `event_json` string,`server_time` string, `ip` string)
PARTITIONED BY (`dt` string)
location '/tmp/weather/ods_basedata_dt/';

use dw_weather;

alter table stage_originlog_lzo_dt add partition(dt='2018-11-03') location '/tmp/flume/weather/2018-11-03';
add jar /home/hdfs/hive-function-1.0-SNAPSHOT.jar;
create temporary function base_analizer as 'com.tom.udf.BaseFieldUDF';
create temporary function flat_analizer as 'com.tom.udtf.EventJsonUDTF';
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table ods_basedata_dt
PARTITION (dt)
select
user_id,
version_code,
version_name,
lang,
source ,
os ,
area ,
model ,
brand ,
sdk_version ,
gmail ,
height_width ,
network ,
lng ,
lat ,
app_time ,
event_name , 
event_json , 
server_time , 
ip ,
dt  
 from
(
select
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[0]   as user_id,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[1]   as version_code,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[2]   as version_name,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[3]   as lang,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[4]   as source,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[5]   as os,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[6]   as area,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[7]   as model,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[8]   as brand,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[9]   as sdk_version,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[10]  as gmail,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[11]  as height_width,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[12]  as network,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[13]  as lng,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[14]  as lat,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[15]  as app_time,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[16]  as ops,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[17]  as server_time,
split(base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t'),'\t')[18]  as ip,
dt 
from stage_originlog_lzo_dt where dt='2018-12-12'  and base_analizer(line,'uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t')<>'' 
) sdk_log lateral view flat_analizer(ops) tmp_k as event_name, event_json;

 


-- 新闻展示表
drop table if exists ods_display_dt;
CREATE EXTERNAL TABLE `ods_display_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,action string,newsid string,place string,showtype string,copyright string,content_provider string,newstype string,extend1 string,extend2 string,category string,`server_time` string, `ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_display_dt/';

insert overwrite table ods_display_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.newsid') newsid,
get_json_object(event_json,'$.kv.place') place,
get_json_object(event_json,'$.kv.showtype') showtype,
get_json_object(event_json,'$.kv.copyright') copyright,
get_json_object(event_json,'$.kv.content_provider') content_provider,
get_json_object(event_json,'$.kv.newstype') newstype,
get_json_object(event_json,'$.kv.extend1') extend1,
get_json_object(event_json,'$.kv.extend2') extend2,
get_json_object(event_json,'$.kv.category') category,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='display';


-- 新闻详情页
drop table if exists ods_newsdetailpro_dt;
CREATE EXTERNAL TABLE `ods_newsdetailpro_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,entry string,action string,newsid string,showtype string,copyright string,content_provider string,newstype string,show_style string,news_staytime string,loading_time string,type1 string,category string,content string,`server_time` string, `ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_newsdetailpro_dt/';

insert overwrite table ods_newsdetailpro_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object('event_json','$.kv.entry') entry,
get_json_object('event_json','$.kv.action') action,
get_json_object('event_json','$.kv.newsid') newsid,
get_json_object('event_json','$.kv.showtype') showtype,
get_json_object('event_json','$.kv.copyright') copyright,
get_json_object('event_json','$.kv.content_provider') content_provider,
get_json_object('event_json','$.kv.newstype') newstype,
get_json_object('event_json','$.kv.show_style') show_style,
get_json_object('event_json','$.kv.news_staytime') news_staytime,
get_json_object('event_json','$.kv.loading_time') loading_time,
get_json_object('event_json','$.kv.type1') type1,
get_json_object('event_json','$.kv.category') category,
get_json_object('event_json','$.kv.content') content,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='newsdetailpro';

-- 新闻列表页
drop table if exists ods_loading_dt;
CREATE EXTERNAL TABLE `ods_loading_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,action string,loading_time string,loading_way string,extend1 string,extend2 string,load_type string,type1 string,`server_time` string,`ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_loading_dt/';

insert overwrite table ods_loading_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object('event_json','$.kv.action') action,
get_json_object('event_json','$.kv.loading_time') loading_time,
get_json_object('event_json','$.kv.loading_way') loading_way,
get_json_object('event_json','$.kv.extend1') extend1,
get_json_object('event_json','$.kv.extend2') extend2,
get_json_object('event_json','$.kv.type') load_type,
get_json_object('event_json','$.kv.type1') type1,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='loading';


--广告
drop table if exists ods_ad_dt;
CREATE EXTERNAL TABLE `ods_ad_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,entry string,action string,content string,detail string,ad_source string,behavior string,newstype string,show_style string,`server_time` string,`ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_ad_dt/';

insert overwrite table ods_ad_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object('event_json','$.kv.entry') entry,
get_json_object('event_json','$.kv.action') action,
get_json_object('event_json','$.kv.content') content,
get_json_object('event_json','$.kv.detail') detail,
get_json_object('event_json','$.kv.source') ad_source,
get_json_object('event_json','$.kv.behavior') behavior,
get_json_object('event_json','$.kv.newstype') newstype,
get_json_object('event_json','$.kv.show_style') show_style,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='ad';

-- 应用启动
drop table if exists ods_start_dt;
CREATE EXTERNAL TABLE `ods_start_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,entry string,open_ad_type string,action string,loading_time string,detail string,extend1 string,`server_time` string,`ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_start_dt/';

insert overwrite table ods_start_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object(event_json,'$.kv.entry') entry,
get_json_object(event_json,'$.kv.open_ad_type') open_ad_type,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.loading_time') loading_time,
get_json_object(event_json,'$.kv.detail') detail,
get_json_object(event_json,'$.kv.extend1') extend1,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='start';

-- 消息通知
drop table if exists ods_notification_dt;
CREATE EXTERNAL TABLE `ods_notification_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,action string,noti_type string,ap_time string,content string,`server_time` string,`ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_notification_dt/';

insert overwrite table ods_notification_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object('event_json','$.kv.action') action,
get_json_object('event_json','$.kv.noti_type') noti_type,
get_json_object('event_json','$.kv.ap_time') ap_time,
get_json_object('event_json','$.kv.content') content,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='notification';


-- 后台活跃表
drop table if exists ods_background_dt;
CREATE EXTERNAL TABLE `ods_background_dt`(`user_id` string, `version_code` string, `version_name` string, `lang` string, `source` string, `os` string, `area` string, `model` string,`brand` string, `sdk_version` string, `gmail` string, `height_width` string,  `network` string, `lng` string, `lat` string, `app_time` string,active_source string,`server_time` string,`ip` string)
PARTITIONED BY (dt string)
location '/tmp/weather/ods_background_dt/';

insert overwrite table ods_background_dt
PARTITION (dt)
select 
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
network,
lng,
lat,
app_time,
get_json_object(event_json,'$.kv.active_source') active_source,
server_time,
ip,
dt
from ods_basedata_dt where dt='2018-12-12' and event_name='active_background';


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

-- 数据量并不大，一天几百万条左右
最近24小时，一个用户，点击多次，算一个点击，一个用户展示多次，算一个展示。

-- 用户24小时点击记录
drop table if exists news_display;
create table news_display(
area varchar(20),
news_id varchar(20),
user_id varchar(20),
time_stam long
);
alter table news_display add unique(area,news_id,user_id);

--用户24小时展示记录
drop table if exists news_click;
create table news_click(
area varchar(20),
news_id varchar(20),
user_id varchar(20),
time_stam long
);
alter table news_click add unique(area,news_id,user_id);

-- 存临时插入的数据
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

--最近的新闻质量表,因为数据量并不大，7天的也就几万条
drop table if exists news_quality;
create table news_quality(
news_id varchar(20) primary key,
quality double,
time_stam int(10) comment '创建秒数' 
);
alter table news_quality add index(news_id);
alter table news_quality add index(time_stam);

select unix_timestamp(now())

-- 初始化脚本 
insert overwrite table news_quality select news_id,rand() quality,unix_timestamp(now()) time_stam from (select distinct news_id from news_click) t1;

--新闻热度
drop table if exists news_heat;
create table news_heat(

)





