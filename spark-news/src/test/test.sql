select count(distinct user_id) num,app_version from mid_user_history_dt
 where dt='2018-09-30' group by app_version order by app_version;

 select count(distinct user_id) num  from mid_user_history_dt
  where dt='2018-09-30' and first_dat=current_dat and first_dat=date_add(dt,-2);


select count(distinct uid) forground_total,dt from dw_foreground where dt='2018-09-25' group by dt;