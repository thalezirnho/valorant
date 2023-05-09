WITH 
MOST_VALUEABLE_PLAYER_PER_MATCH AS (
  SELECT matchid,
  		 puuid,
  		 TRUE AS match_mvp
  	FROM tabular_valorant_match.player_stats
   WHERE TRUE
 QUALIFY ROW_NUMBER() OVER(PARTITION BY matchid ORDER BY score DESC) = 1
)
, MATCHS_WITH_FULL_TEAM AS (
  SELECT matchid, COUNT(DISTINCT puuid) = 5 AS full_team
  	FROM tabular_valorant_match.player 
   WHERE TRUE
     AND puuid IN ( "f2f7c867-27ba-5291-a23c-bbdcfc079e76",
					"35a2368e-586f-53ec-8ee8-ea9ebdca7d6f",
					"2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
					"6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
					"b2310858-1741-5d6f-8499-cb1f85bd182a")
GROUP BY 1
)
  SELECT DISTINCT
  		 m.matchid,
  		 m.map,
  		 TIMESTAMP_SECONDS(m.game_start) AS game_start,
  		 m.rounds_played,
  		 t.red_has_won,
  		 t.red_rounds_won,
  		 t.red_rounds_lost,
  		 t.blue_has_won,
  		 t.blue_rounds_won,
  		 t.blue_rounds_lost,
  		 p.name,
  		 p.team,
  		 p.level,
  		 CASE
	  		 WHEN (t.red_has_won IS TRUE AND p.team = 'Red') OR (t.blue_has_won IS TRUE AND p.team = 'Blue')
	  		 THEN TRUE 
	  		 ELSE FALSE
	  	 END AS has_won_the_match,
  		 p.damage_made,
  		 p.damage_received,
  		 p.currenttier,
  		 p.character,
  		 pa.agent_small,
  		 pa.agent_killfeed,
  		 COALESCE(MVP.match_mvp, FALSE) AS match_mvp,
  		 ps.* EXCEPT(matchid, puuid),
  		 ps.bodyshots + ps.headshots + ps.legshots AS total_shots,
  		 pe.* EXCEPT(matchid, puuid),
  		 pac.* EXCEPT(matchid, puuid),
  		 ROW_NUMBER() OVER(PARTITION BY p.puuid ORDER BY m.game_start DESC) = 1 AS last_game,
  		 FT.full_team
  	FROM tabular_valorant_match.metadata AS m
  		 LEFT JOIN tabular_valorant_match.teams AS t
  		 ON m.matchid = t.matchid 
  		 LEFT JOIN tabular_valorant_match.player AS p 
  		 ON m.matchid = p.matchid
  		 LEFT JOIN tabular_valorant_match.player_stats AS ps
  		 ON m.matchid = ps.matchid 
  		 	AND p.puuid = ps.puuid 
  		 LEFT JOIN tabular_valorant_match.player_economy AS pe
  		 ON m.matchid = pe.matchid 
  		 	AND p.puuid = pe.puuid 
  		 LEFT JOIN tabular_valorant_match.player_ability_casts AS pac 
  		 ON m.matchid = pac.matchid 
  		 	AND p.puuid = pac.puuid 
  		 LEFT JOIN MOST_VALUEABLE_PLAYER_PER_MATCH AS MVP
  		 ON m.matchid = MVP.matchid
  		 	AND p.puuid = MVP.puuid
  		 LEFT JOIN tabular_valorant_match.player_assets AS pa
  		 ON m.matchid = pa.matchid
  		 	AND p.puuid = pa.puuid
  		 LEFT JOIN MATCHS_WITH_FULL_TEAM AS FT
  		 ON m.matchid = FT.matchid
   WHERE TRUE
     AND p.name IN ('viikset', 'mBirth', 'TADALA', 'Kubata', 'DIGAOTJS')
