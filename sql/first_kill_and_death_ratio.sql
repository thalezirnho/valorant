-- First Blood Ratio
WITH 
ROUND_PLAYED AS (
  SELECT p.name,
  		 p.tag,
  		 p.puuid,
  		 SUM(m.rounds_played) AS rounds_played
  	FROM tabular_valorant_match.player AS p
  		 LEFT JOIN tabular_valorant_match.metadata AS m
  		 ON p.matchid = m.matchid 
   WHERE TRUE
     AND p.puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     			   	 "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     			   	 "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     			   	 "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     			   	 "b2310858-1741-5d6f-8499-cb1f85bd182a")
GROUP BY 1, 2, 3
)
, ALL_FIRST_BLOODS AS (
  SELECT DISTINCT
  		 matchid,
  		 round,
  		 killer_display_name,
  		 killer_puuid,
  		 victim_display_name,
  		 victim_puuid
   	FROM tabular_valorant_match.kills 
QUALIFY ROW_NUMBER() OVER(PARTITION BY matchid, round ORDER BY kill_time_in_round ASC) = 1
)
, OUR_FIRST_BLOODS AS (
  SELECT killer_puuid AS puuid,
  		 COUNT(DISTINCT CONCAT(matchid, round)) AS first_blood
  	FROM ALL_FIRST_BLOODS
   WHERE TRUE
     AND killer_puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     			   	 	  "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     			   	 	  "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     			   	 	  "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     			   	 	  "b2310858-1741-5d6f-8499-cb1f85bd182a")
GROUP BY 1
)
, OUR_FIRST_DEATH AS (
  SELECT victim_puuid AS puuid,
  		 COUNT(DISTINCT CONCAT(matchid, round)) AS first_death
  	FROM ALL_FIRST_BLOODS
   WHERE TRUE
     AND victim_puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     			   	 	  "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     			   	 	  "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     			   	 	  "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     			   	 	  "b2310858-1741-5d6f-8499-cb1f85bd182a")
GROUP BY 1
)
  SELECT R.name,
  		 ROUND(FB.first_blood/R.rounds_played, 3) AS ratio_first_blood,
  		 ROUND(FD.first_death/R.rounds_played, 3) AS ratio_first_death,
  		 ROUND(FB.first_blood/R.rounds_played, 3)/ ROUND(FD.first_death/R.rounds_played, 3) AS ratio,
  		 R.rounds_played,
  		 FB.first_blood,
  		 FD.first_death,
  	FROM ROUND_PLAYED AS R
  		 LEFT JOIN OUR_FIRST_BLOODS AS FB 
  		 ON R.puuid = FB.puuid
  		 LEFT JOIN OUR_FIRST_DEATH AS FD
  		 ON R.puuid = FD.puuid

  	
  	
  	