-- 1. Quais jogos do gênero ‘RPG’ lançados nos últimos 6 meses têm ≥ 80% de avaliações positivas e ≥ 10.000 avaliações no total?
SELECT g.name,
       g.release_date,
       m.positive,
       m.negative,
       (m.positive * 100.0 / (m.positive + m.negative)) AS percentual_positivo
FROM silver.games g
JOIN silver.metrics m 
    ON g.game_id = m.game_id
JOIN silver.game_genre gg 
    ON g.game_id = gg.game_id
JOIN silver.genres gen 
    ON gg.genre_id = gen.genre_id
WHERE gen.name = 'RPG'
  AND g.release_date >= CURRENT_DATE - INTERVAL '6 months'
  AND (m.positive + m.negative) >= 10000
  AND (m.positive * 100.0 / (m.positive + m.negative)) >= 80;

-- 2. Quais jogos “cross-platform” (Windows, Mac e Linux) custam ≤ 20 (na moeda do dataset) e têm média de jogo (average_playtime) ≥ 120 minutos?
SELECT g.name,
       g.price,
       g.windows,
       g.mac,
       g.linux,
       m.average_playtime_forever
FROM silver.games g
JOIN silver.metrics m
    ON g.game_id = m.game_id
WHERE g.windows = TRUE
  AND g.mac = TRUE
  AND g.linux = TRUE
  AND g.price <= 20
  AND m.average_playtime_forever >= 120;

-- 3. Quais desenvolvedores lançaram mais de 5 jogos nos últimos 12 meses e qual a média do score de avaliações desses lançamentos?
SELECT d.name AS developer,
       COUNT(g.game_id) AS total_jogos,
       AVG(m.user_score) AS media_score
FROM silver.games g
JOIN silver.developers d
    ON g.developer_id = d.developer_id
JOIN silver.metrics m
    ON g.game_id = m.game_id
WHERE g.release_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY d.name
HAVING COUNT(g.game_id) > 5
ORDER BY media_score DESC;

-- 4. Quantos jogos pagos lançados nos últimos 3 meses têm mediana de tempo de jogo (median_playtime) > 240 min, e quais são os 5 gêneros mais frequentes entre eles?
WITH jogos_filtrados AS (
    SELECT g.game_id, g.name
    FROM silver.games g
    JOIN silver.metrics m
        ON g.game_id = m.game_id
    WHERE g.price > 0
      AND g.release_date >= CURRENT_DATE - INTERVAL '3 months'
      AND m.median_playtime_forever > 240
),
total AS (
    SELECT COUNT() AS total_jogos
    FROM jogos_filtrados
)
SELECT t.total_jogos,
       gen.name AS genero,
       COUNT() AS frequencia
FROM jogos_filtrados jf
JOIN silver.game_genre gg
    ON jf.game_id = gg.game_id
JOIN silver.genres gen
    ON gg.genre_id = gen.genre_id
CROSS JOIN total t
GROUP BY t.total_jogos, gen.name
ORDER BY frequencia DESC, gen.name
LIMIT 5;

-- 5. Qual é o “top 1” de tags mais frequente entre os 100 jogos com melhor score de avaliação (considerando proporção positivas/(positivas+negativas)) nos últimos 6 meses?
WITH top_games AS (
    SELECT g.game_id,
           (m.positive * 1.0 / NULLIF(m.positive + m.negative, 0)) AS score
    FROM silver.games g
    JOIN silver.metrics m
        ON g.game_id = m.game_id
    WHERE g.release_date >= CURRENT_DATE - INTERVAL '6 months'
    ORDER BY score DESC
    LIMIT 100
)

SELECT t.name AS tag,
       COUNT(*) AS frequencia
FROM top_games tg
JOIN silver.tags_games tg2
    ON tg.game_id = tg2.game_id
JOIN silver.tags t
    ON tg2.tags_id = t.tag_id
GROUP BY t.name
ORDER BY frequencia DESC
LIMIT 1;