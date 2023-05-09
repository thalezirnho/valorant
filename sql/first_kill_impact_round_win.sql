-- First Blood vs Winning Rounds
WITH
START_ON_ATTACK AS (
  SELECT DISTINCT 
  		 matchid,
  		 CASE
  		 	 WHEN team = 'Red'
  		 	 THEN TRUE
  		 	 ELSE FALSE
  		 END AS start_on_attack
  	FROM tabular_valorant_match.player
   WHERE TRUE
     AND puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     			   "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     			   "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     			   "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     			   "b2310858-1741-5d6f-8499-cb1f85bd182a")
)
, WE_WON AS (
  SELECT DISTINCT 
  	  	 p.matchid,
  		 p.team,
  		 CASE 
  		 	 WHEN t.red_has_won IS FALSE AND t.blue_has_won IS FALSE
  		 	 THEN FALSE
  		 	 WHEN p.team = 'Red' AND t.red_has_won IS TRUE 
  		 	 THEN TRUE
  		 	 WHEN p.team = 'Blue' AND t.blue_has_won IS TRUE 
  		 	 THEN TRUE
  		 	 ELSE FALSE
  		 END AS we_won,
  		 t.red_has_won,
  		 t.blue_has_won 
  	FROM tabular_valorant_match.player AS p
  		 LEFT JOIN tabular_valorant_match.teams AS t
  		 ON p.matchid = t.matchid 
   WHERE TRUE
     AND p.puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     			   	 "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     			   	 "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     			   	 "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     			   	 "b2310858-1741-5d6f-8499-cb1f85bd182a")
)
, FIRST_BLOOD AS (
  SELECT DISTINCT
  		 matchid,
  		 round,
  		 killer_team
   	FROM tabular_valorant_match.kills 
QUALIFY ROW_NUMBER() OVER(PARTITION BY matchid, round ORDER BY kill_time_in_round ASC) = 1
)
, CONSOLIDATED AS (
  SELECT DISTINCT 
  		 m.map,
  		 m.matchid,
  		 SOA.start_on_attack,
  		 WE_WON.we_won,
  		 WE_WON.team,
  		 WE_WON.team = r.winning_team AS we_won_round,
  		 r.* EXCEPT(end_type, winning_team, matchid, round_id),
  		 FB.killer_team = WE_WON.team AS we_have_first_blood
  	FROM tabular_valorant_match.metadata AS m
  		 LEFT JOIN START_ON_ATTACK AS SOA
  		 ON m.matchid = SOA.matchid
  		 LEFT JOIN WE_WON
  		 ON m.matchid = WE_WON.matchid
  		 LEFT JOIN tabular_valorant_match.round AS r
  		 ON m.matchid = r.matchid
  		 LEFT JOIN FIRST_BLOOD AS FB
  		 ON m.matchid = FB.matchid
  		 	AND r.round = FB.round
   WHERE TRUE
)
, VICTORY AS (
  SELECT map,
  		 we_won_round,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS TRUE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_true,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS FALSE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_false
  	FROM CONSOLIDATED
   WHERE TRUE
     AND we_won_round IS TRUE
GROUP BY 1, 2
)
, DEFEAT AS (
  SELECT map,
  		 we_won_round,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS TRUE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_true,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS FALSE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_false
  	FROM CONSOLIDATED
   WHERE TRUE
     AND we_won_round IS FALSE
GROUP BY 1, 2
)
  SELECT map,
  		 we_won_round,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS TRUE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_true,
  		 COUNT(DISTINCT CASE WHEN we_have_first_blood IS FALSE THEN CONCAT(matchid, round) ELSE NULL END) AS fb_false,
  		 ROUND(COUNT(DISTINCT CASE WHEN we_have_first_blood IS TRUE THEN CONCAT(matchid, round) ELSE NULL END) / COUNT(DISTINCT CONCAT(matchid, round)), 2) ratio
  	FROM CONSOLIDATED
   WHERE TRUE
GROUP BY 1, 2

     
     
