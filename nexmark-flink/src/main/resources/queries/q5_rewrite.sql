
CREATE TABLE nexmark_q5_rewrite (
    auction  BIGINT,
    num  BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q5_rewrite
SELECT
    B.auction,
    B.num
FROM (
         SELECT
             auction,
             num,
             row_number() over (partition by starttime order by num desc) as numRank
         FROM (
                  SELECT
                      auction,
                      count(*) AS num,
                      window_start AS starttime
                  FROM TABLE(
                          HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
                  GROUP BY auction, window_start, window_end
              )
     ) B
where B.numRank <= 1;