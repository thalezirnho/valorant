
WITH ALL_SHOOTS_PER_WEAPON AS (
  SELECT player_puuid,
  		 player_display_name,
  		 economy_weapon_name,
  		 SUM(headshots) AS headshots,
  		 SUM(bodyshots) AS bodyshots,
  		 SUM(legshots) AS legshots,
  		 SUM(headshots) + SUM(bodyshots) + SUM(legshots) AS allshots
  	FROM tabular_valorant_match.round_player_stats
   WHERE TRUE 
     AND player_puuid IN ("f2f7c867-27ba-5291-a23c-bbdcfc079e76",
     					  "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f", 
     					  "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
     					  "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
     					  "b2310858-1741-5d6f-8499-cb1f85bd182a")
GROUP BY 1, 2, 3
)
  SELECT player_puuid,
  		 player_display_name,
  		 economy_weapon_name,
  		 headshots,
  		 allshots,
  		 ROUND(SAFE_DIVIDE(headshots, allshots)*100, 2) AS percentage_headshots
  	FROM ALL_SHOOTS_PER_WEAPON