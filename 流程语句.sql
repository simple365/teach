
--app用户历史表
drop table if exists mid_user_history_dt;
CREATE EXTERNAL TABLE `mid_user_history_dt`(`user_id` string, `area` string,version_name string,lang string,source string,first_version string,`first_dat` string, first_source string,`last_dat` string)
comment '用户历史表'
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_user_history_dt/';

--初始化语句
insert overwrite table mid_user_history_dt
partition (dt)
select user_id,
area,
version_name,
lang,
source,
first_version,
first_dat,
first_source,
last_dt,dt from (select 
row_number() over (partition by user_id order by server_time) rn,
user_id,
area,
version_name,
lang,source,
version_name first_version,
dt first_dat,
source first_source,
dt last_dt,
dt
from ods_basedata_dt where dt='2018-11-03') t where rn=1;

--比较两个用户表
-- 先创建一个临时表，去掉重复数据
drop table if exists tmp_user_today;
create table tmp_user_today as 
select user_id,
area,
version_name,
lang,
source,
first_version,
first_dat,
first_source,
last_dat,dt from (select 
row_number() over (partition by user_id order by server_time) rn,
user_id,
area,
version_name,
lang,source,
version_name first_version,
dt first_dat,
source first_source,
dt last_dat,
dt
from ods_basedata_dt where dt='2018-11-08') t where rn=1;

-- 再清洗
add jar /home/hadoop/hive-function-1.0-SNAPSHOT.jar;
create temporary function first_val as 'com.tom.udaf.FirstValueDateUDAF';
set hive.exec.dynamic.partition.mode=nonstrict;

use dw_weather;

insert overwrite table mid_user_history_dt partition (dt)
select user_id, 
split(first_val(current_info,"desc"),",")[1] area,
split(first_val(current_info,"desc"),",")[2] version_name,
split(first_val(current_info,"desc"),",")[3] lang,
split(first_val(current_info,"desc"),",")[4] source,
split(first_val(old_info),",")[0] first_dat,
split(first_val(old_info),",")[1] first_version,
split(first_val(old_info),",")[2] first_source,
split(first_val(current_info,"desc"),",")[0] last_dat,
'2018-11-08' dt 
from (
select 
user_id,
concat_ws(',',last_dat,area,version_name,lang,source) current_info,
concat_ws(',',first_dat,first_version,first_source) old_info
from mid_user_history_dt where dt='2018-11-03'
union
select 
user_id,
concat_ws(',',last_dat,area,version_name,lang,source) current_info,
concat_ws(',',first_dat,first_version,first_source) old_info 
from tmp_user_today ) t group by user_id;


-------------- 新闻每日展示点击详情表 -------------------
drop table if exists mid_daily_news_dt;
CREATE EXTERNAL TABLE `mid_daily_news_dt`(`news_id` string, lang string,version_name string,area string,total_display int,total_click int)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_daily_news_dt/';

-- 创建点击和展示的详细临时表
drop table if exists tmp_news_display;
create table tmp_news_display as 
select newsid,lang,version_name,area,user_id from ods_display_dt 
where dt='2018-11-08' and action='1';

drop table if exists tmp_news_click;
create table tmp_news_click as 
select newsid,lang,version_name,area,user_id from ods_display_dt 
where dt='2018-11-08' and action='2';

-- 将数据插入
insert overwrite table mid_daily_news_dt 
partition(dt='2018-11-08')
select t1.*,t2.total_click from (
select newsid news_id,
lang,version_name,area,count(distinct user_id) total_display from tmp_news_display 
group by newsid,lang,version_name,area
) t1 
join 
(select newsid news_id,
lang,version_name,area,count(distinct user_id) total_click  from tmp_news_click 
group by newsid,lang,version_name,area
) t2
on t1.news_id=t2.news_id and t1.lang=t2.lang and t1.version_name=t2.version_name;

--计算新闻的总点击总展示
select t3.*,t2.total_display3,t2.total_click3,t1.total_display,t1.total_click from (
select news_id,sum(total_display) total_display,sum(total_click) total_click 
from mid_daily_news_dt where dt='2018-11-08' group by news_id
) t1 right join
(select news_id,sum(total_display) total_display3,sum(total_click) total_click3 
from mid_daily_news_dt where dt>date_add('2018-11-08',-3) group by news_id) t2 
on t1.news_id=t2.news_id
right join
(select news_id,sum(total_display) total_display7,sum(total_click) total_click7 
from mid_daily_news_dt where dt>date_add('2018-11-08',-7) group by news_id) t3
on t1.news_id=t3.news_id;
 
--计算新闻的地区总点击总展示
select t3.*,t2.total_display3,t2.total_click3,t1.total_display,t1.total_click from (
select news_id,area,sum(total_display) total_display,sum(total_click) total_click 
from mid_daily_news_dt where dt='2018-11-08' group by news_id,area
) t1 right join
(select news_id,area,sum(total_display) total_display3,sum(total_click) total_click3 
from mid_daily_news_dt where dt>date_add('2018-11-08',-3) group by news_id,area) t2 
on t1.news_id=t2.news_id and t1.area=t2.area 
right join
(select news_id,area,sum(total_display) total_display7,sum(total_click) total_click7 
from mid_daily_news_dt where dt>date_add('2018-11-08',-7) group by news_id,area) t3
on t1.news_id=t3.news_id and t1.area=t2.area;

--前台，后台活跃用户数统计(日活，月活)
drop table if exists dm_active_dt;
create table dm_active_dt (
day_foreground int,
month_foreground int,
day_background int,
month_background int
)PARTITIONED BY (`dt` string)
location '/tmp/teach/dm_active_dt/';

insert overwrite table dm_active_dt
partition(dt) 
select t1.day_foreground,t3.month_foreground,t2.day_background,t4.month_background,t1.dt from (
select count(distinct user_id) day_foreground,dt from ods_start_dt where dt='2018-11-08' and action='1' group by dt
) t1 join  
(select count(distinct user_id) month_foreground from ods_start_dt where dt>='2018-11-01' and dt<='2018-11-08' and action='1') t3 on 1=1
join 
(select count(distinct user_id) day_background,dt from ods_background_dt where dt='2018-11-08' group by dt) t2 on t1.dt=t2.dt
join 
(select count(distinct user_id) month_background from ods_background_dt where dt>='2018-11-01' and dt<='2018-11-08' ) t4 on 1=1;

-- 用户版本分布
drop table if exists dm_version_allocation;
create table dm_version_allocation (
version_name string,
user_num_fore int,
user_num_back int
)PARTITIONED BY (`dt` string)
location '/tmp/teach/dm_version_allocation/';

insert overwrite table dm_version_allocation
partition(dt='2018-11-08') 
select t1.*,t2.user_num_back from (
select version_name,count(distinct user_id) user_num_fore from ods_start_dt where dt='2018-11-08' and action='1' group by version_name
) t1 join  
(select version_name,count(distinct user_id) user_num_back from ods_background_dt where dt='2018-11-08' group by version_name) t2 on t1.version_name=t2.version_name order by t1.version_name;

-- 新闻历史表
drop table if exists mid_news_history_dt;
CREATE EXTERNAL TABLE `mid_news_history_dt`(`news_id` string, `area` string, `first_display_time` string,`last_display_time` string,`first_click_time` string,`last_click_time` string,total_display int,total_click int)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_news_history_dt/';






--活跃用户表
drop table if exists mid_active_user_dt;
CREATE EXTERNAL TABLE `mid_active_user_dt`(`user_id` string, `area` string)
PARTITIONED BY (`dt` string)
location '/tmp/teach/mid_active_user_dt/';









-- 数据倾斜测试脚本
drop table if exists skew1;
create table skew1 as 
select id,val from (select  cast(rand()*8 as Integer) id ,rand() val from  app_center.stage_homeland_ac_originlog_lzo_dt limit 700000)
union 
select id,val from (select '' id ,rand() val from app_center.stage_homeland_ac_originlog_lzo_dt limit 300000);

drop table if exists skew2;
create table skew2 as 
select id,val from (select  cast(rand()*8 as Integer) id ,rand() val from  app_center.stage_homeland_ac_originlog_lzo_dt limit 800000)
union 
select id,val from (select '' id ,rand() val from app_center.stage_homeland_ac_originlog_lzo_dt limit 200000);

explain  
select id,count(0) from 
(select id,val from (select  cast(rand()*8 as Integer) id ,rand() val from  app_center.stage_homeland_ac_originlog_lzo_dt limit 800000)  union select id,val from (select '' id ,rand() val from app_center.stage_homeland_ac_originlog_lzo_dt limit 200000) )
group by id;

--order by 
-- 查看倾斜里面的数据
select * from (select row_number() over(order by id) rn ,* from skew1) t where t.rn>10000 and t.rn<10100;
-- 查看非倾斜里面的数据
select * from (select row_number() over(order by id) rn ,* from skew1) t where t.rn>800000 and t.rn<800100;




== Physical Plan ==
*Project [rn#91, id#94, val#95]
+- *Filter ((isnotnull(rn#91) && (rn#91 > 10000)) && (rn#91 < 10100))
   +- Window [row_number() windowspecdefinition(id#94 ASC NULLS FIRST, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn#91], [id#94 ASC NULLS FIRST]
      +- *Sort [id#94 ASC NULLS FIRST], false, 0
         +- Exchange SinglePartition
            +- HiveTableScan [id#94, val#95], MetastoreRelation teach, skew1


select sum(news_id) news_total,sum(display)/sum(news_id) avg_display,sum(click)/sum(news_id) avg_click, sum(display)/sum(click) avg_rate
from (
select
 t1.display display,
 case when t2.click is not null then t2.click else 0 end click,
 t1.news_id
 from (select count(distinct user_id) display,news_id from user_display group by news_id) t1 left join
(select count(distinct user_id) click,news_id from user_click group by news_id) t2 on
t1.news_id=t2.news_id
) t3
			
			


			

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

-- 计算每日新闻展现用户数，展现次数，
select user_num,display_num,display_num/(user_num*1.0) rate from (
select count(distinct user_id) user_num from ods_display_dt where dt='2018-11-03' ) t1 cross join 
(select count(0) display_num from ods_display_dt where dt='2018-11-03' ) t2;
 

-- 曝光新闻次数,人均曝光新闻次数
select news_display_num,user_num,news_display_num/user_num avg_display_news from (
select count(0) news_display_num from ods_display_dt where dt='2018-11-08' and action='1') t1 cross join
(select count(distinct user_id) user_num from ods_display_dt where dt='2018-11-03' and action='1') t2

-- 人均曝光新闻条数
select news_num,user_num,news_num/user_num avg_display_news from (
select count(distinct news_id) news_num from ods_display_dt where dt='2018-11-08' and action='1') t1 cross join
(select count(distinct user_id) user_num from ods_display_dt where dt='2018-11-03' and action='1') t2

-- 点击新闻用户数
select count(distinct user_id) click_user_num from ods_display_dt where dt='2018-11-03' and action='2';

-- 点击新闻条数
select count(distinct news_id) click_news_num from ods_display_dt where dt='2018-11-03' and action='2';

-- 点击新闻次数
select count(0) click_news_times from ods_display_dt where dt='2018-11-03' and action='2';

-- 人均点击新闻次数
select click_user_num,click_news_times,(click_news_times*1.0)/click_user_num avg_click_times from 
(select count(distinct user_id) click_user_num from ods_display_dt where dt='2018-11-03' and action='2') t1 cross join
(select count(0) click_news_times from ods_display_dt where dt='2018-11-03' and action='2') t2 


-- 人均点击新闻量
select click_user_num,click_news_num,click_news_num/(click_user_num*1.0) avg_click_amount from 
(select count(distinct user_id) click_user_num from ods_display_dt where dt='2018-11-03' and action='2') t1 cross join
(select count(distinct news_id) click_news_num from ods_display_dt where dt='2018-11-03' and action='2') t2 

-- 当天新闻总点击率
select display_num,click_num,display_num/click_num from 
(select count(distinct user_id) click_news_num from ods_display_dt where dt='2018-11-03' and action='2') t1 cross join 

--- 计算用户历史的脚本
insert overwrite table mid_user_history_dt 
partition (dt='2018-09-30')
select user_id,area,max(app_version) app_version,min(first_dat) first_dat,max(last_dat) last_dat from (
select distinct user_id,area,dt first_dat,dt last_dat from ods_basedata_dt where dt='2018-09-30' 
union
select user_id,area,app_version,first_dat,last_dat from mid_user_history_dt where dt='2018-09-29' 
)group by user_id,area;

-- 当天所有新闻总点击率 


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

