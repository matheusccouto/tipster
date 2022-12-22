WITH clubs AS (
  SELECT DISTINCT home_team AS team, sport_key FROM theoddsapi.odds
  UNION ALL 
  SELECT DISTINCT away_team AS team, sport_key FROM theoddsapi.odds
)
SELECT team FROM clubs
QUALIFY ROW_NUMBER() OVER (PARTITION BY team ORDER BY sport_key DESC) = 1
ORDER BY sport_key, team