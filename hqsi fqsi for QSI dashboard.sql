with
cte_example as (
select distinct market, left(market,3) as origin, right(market,3) as destination, carrier_code, stops
from svc.qsi_scores s
where
	s.market = 'JFK-LHR' and
	s.qsi_date = '2019-06-01' and
	s.score_under_threshold = false
union
select distinct market, left(market,3) as origin, right(market,3) as destination, carrier_code, stops
from svc.qsi_scores s
where
	s.market = 'JFK-LHR' and
	s.qsi_date = (select max(qsi_date) from svc.qsi_scores) and
	s.score_under_threshold = false),
cte_agg as (
select
	s.qsi_date as hqsi_date,
	f.fqsi_date,
	c1.*,
	coalesce(h.hqsi_score,0) as hqsi_score,
	coalesce(f.fqsi_score,0) as fqsi_score	
from cte_example c1
cross join (select distinct qsi_date from svc.qsi_scores where qsi_date = '2019-06-01') s
left join (
	select s.qsi_date, s.market, s.carrier_code, s.stops, s.score as hqsi_score
	from svc.qsi_scores s
	where
		s.score_under_threshold is false and
		s.market = 'JFK-LHR' and
		s.qsi_date = '2019-06-01') h on
			h.qsi_date = s.qsi_date and
			h.market = c1.market and
			h.carrier_code = c1.carrier_code and
			h.stops = c1.stops
left join (
	select s.qsi_date as fqsi_date, s.market, s.carrier_code, s.stops, s.score as fqsi_score
	from svc.qsi_scores s
	where
		s.score_under_threshold is false and
		s.market = 'JFK-LHR' and
		s.qsi_date = (select max(qsi_date) from svc.qsi_scores)) f on
			f.market = c1.market and
			f.carrier_code = c1.carrier_code and
			f.stops = c1.stops)