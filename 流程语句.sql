--计算新闻历史，需要找出新闻最大，最小，自动忽略null值，
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



--- 计算用户历史的脚本
insert overwrite table mid_user_history_dt 
partition (dt='2018-09-30')
select user_id,area,max(app_version) app_version,min(first_dat) first_dat,max(last_dat) last_dat from (
select distinct user_id,area,dt first_dat,dt last_dat from ods_basedata_dt where dt='2018-09-30' 
union
select user_id,area,app_version,first_dat,last_dat from mid_user_history_dt where dt='2018-09-29' 
)group by user_id,area;


-- 7天内产生点击的用户为活跃用户
insert overwrite table mid_active_user_dt
partition (dt='2018-09-30')
select distinct user_id,area from  ods_display_dt where action='2' and dt>=date_add(current_date,-7);

--计算用户版本分布
select count(distinct user_id) num,app_version from mid_user_history_dt where dt='2018-09-30' group by app_version order by app_version 


--前后台统计
select t1.forground_total,t2.background_total,t1.dt from
(select count(distinct uid) forground_total,dt from dw_foreground where dt='2018-09-25' group by dt) t1
join (select count(distinct uid) background_total,dt from dw_background where dt='2018-09-25' group by dt) t2
on t1.dt=t2.dt ;

--沉默用户统计
select count(distinct user_id) num  from mid_user_history_dt
 where dt='2018-09-30' and first_dat=current_dat and first_dat=date_add(dt,-2);

--新鲜度分析
用户新鲜度 = 某段时间的新增用户数/某段时间的活跃的用户数
今天新增用户（为 n）
select count(user_id),dt new_users from dw_history_users where dt='2018-09-26' and current_dat=dt group by dt;
今天活跃用户（m）
select count(distinct uid) forground_total,dt from dw_foreground where dt='2018-09-26' group by dt;

select forground_total/new_users fresh_rate,t1.dt from 
(select count(user_id),dt new_users from dw_history_users where dt='2018-09-26' and current_dat=dt group by dt) t1 
join 
(select count(distinct uid) forground_total,dt from dw_foreground where dt='2018-09-26' group by dt) t2 
on t1.dt=t2.dt;

----+++++++++++++++++++++++数据流程基本语句
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




-- 新闻展示和点击表
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

-- 前台活跃明细表
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

--后台活跃明细表
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


--每日新增：
select count(user_id) from dw_history_users where dt='2018-09-26' and current_dat=dt

--每周新增：
select count(user_id) from dw_history_users where dt='2018-09-26' and first_dat<=dt
and first_dat> date_add(dt, -7)

--每月新增：
select count(user_id) from dw_history_users where dt='2018-09-26' and first_dat<=dt
and first_dat> date_add(dt, -30)



-- *********************************************** mysql 语句


select t1.*,case when t2.click is not null then t2.click else 0 end click  from (select count(distinct user_id) display,news_id,area from news_display group by area,news_id) t1 left join (select count(distinct user_id) click ,news_id,area from news_click group by area,news_id) t2 on t1.news_id = t2.news_id;

