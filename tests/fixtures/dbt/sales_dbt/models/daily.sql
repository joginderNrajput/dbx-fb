select region, sum(amount) as amount from main.gold.daily_sales
group by region
