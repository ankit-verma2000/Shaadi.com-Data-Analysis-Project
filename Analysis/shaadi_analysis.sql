/*
1. Monthly match conversion rate and the avg no. of new matches created per day on shaadi.com
*/
with match_status as (
select month(match_date) as months,
DAY(EOMONTH(match_date)) AS DaysInMonth,
sum(case when status = 'Active' then 1 else 0 end ) as  active_status,
sum(case when status = 'Closed' then 1 else 0 end) as  closed_status,
count(*) as total_status
from matches
group by month(match_date),
DAY(EOMONTH(match_date))
)
select months,
concat(
	round((cast(active_status as float)/nullif(cast(total_status as Float),0)) *100,0),'%') as monthly_match_rate_ptc,
round(cast(active_status as float)/ DaysInMonth,0) as daily_match_count
from match_status;

/*
2. write the sql query to calculate the average no. of messages sent
per match and the avg total messages sent per day on shaadi.com
*/
with matched_messages as ( -- get only the messages exchanged between users who are matched
select m.match_id, mg.message_id, mg.message_date
from matches as m
join messages as mg
on mg.sender_id in (m.user1_id, m.user2_id) -- sender must be 1 person ion the match
and mg.receiver_id in (m.user1_id, m.user2_id) -- receiver must be other person in match
), 
messages_per_match as (
select match_id, count(message_id) as msg_count
from matched_messages
group by match_id
),
messages_per_day as (
select cast(message_date as date) as msg_date,
count(*) as daily_msg
from messages
group by cast(message_date as date)
)
select (
select avg(msg_count * 1.0) 
from messages_per_match) as avg_message_per_match,
(select avg(daily_msg * 1.0) from messages_per_day) as avg_message_per_day;

/*
# 3 .Find the month-over-month growth % of new users joining Shaadi.com.
*/
with cte1 as (
select month(join_date) as months,
datename(month, join_date) as month_name,
count(user_id) as new_user
from users
group by month(join_date), datename(month, join_date)
),
cte2 as (
select months, month_name, new_user,
lag(new_user) over(order by months) as prev_month_user
from cte1
)
select month_name as Month,
new_user, 
case
when prev_month_user is null then null
else CONCAT(
    CAST(ROUND(((new_user - prev_month_user) * 1.0 / prev_month_user) * 100, 2) AS DECIMAL(10,2)),
    '%'
) 
end AS mom_growth_pct
-- concat(ROUND( ((new_user - prev_month_user) * 1.0 / prev_month_user) * 100 , 2), '%')
from cte2
order by months;



/*
# 4.Identify months with a 2-month continuous decline in total matches created.
*/
with monthly_match_counts as (
select --MONTH(match_date) as month_no,
datename(month ,match_date) as month_name,
count(user1_id) current_month_count
from matches
where status = 'Active'
group by --MONTH(match_date) ,
datename(month ,match_date)
),
monthly_decline_check as (
select *, lag(current_month_count) over(order by month_name) as prev_month_count,
lag(current_month_count,2) over(order by month_name) as prev2_month_count
from monthly_match_counts
)
select month_name, prev2_month_count,prev_month_count,
current_month_count
from monthly_decline_check
where prev2_month_count > prev_month_count
and prev_month_count > current_month_count;
