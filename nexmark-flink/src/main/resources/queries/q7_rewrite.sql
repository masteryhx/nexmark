
CREATE TABLE nexmark_q7_rewrite (
    auction  BIGINT,
    price  BIGINT,
    bidder  BIGINT,
    dateTime  TIMESTAMP(3)
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q7_rewrite
SELECT
    B.auction,
    B.price,
    B.bidder,
    B.dateTime
FROM (
         SELECT
             B2.auction,
             B2.price,
             B2.bidder,
             B2.dateTime,
             /*use rank here to express top-N with ties*/
             row_number() over (partition by B2.window_end order by B2.price desc) as priceRank
         FROM (
                  SELECT auction, price, bidder, dateTime, window_end
                  FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' MINUTES))
              ) B2
     ) B
WHERE B.priceRank <= 1;
