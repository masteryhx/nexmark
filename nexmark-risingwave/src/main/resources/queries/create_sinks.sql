CREATE SINK nexmark_q0 AS
SELECT auction, bidder, price, datetime, extra
FROM bid
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q1 AS
SELECT auction,
       bidder,
       0.908 * price as price,
       datetime,
       extra
FROM bid
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q2 AS
SELECT auction, price
FROM bid
WHERE MOD(auction, 123) = 0
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q3 AS
SELECT P.name,
       P.city,
       P.state,
       A.id
FROM auction AS A
         INNER JOIN person AS P on A.seller = P.id
WHERE A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q4 AS
SELECT Q.category,
       AVG(Q.final) as avg
FROM (SELECT MAX(B.price) AS final,
    A.category
    FROM auction A,
    bid B
    WHERE A.id = B.auction
    AND B.datetime BETWEEN A.datetime AND A.expires
    GROUP BY A.id, A.category) Q
GROUP BY Q.category
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q5 AS
SELECT
    AuctionBids.auction, AuctionBids.num
FROM (
         SELECT
             bid.auction,
             count(*) AS num,
             window_start AS starttime,
             window_end AS endtime
         FROM
             HOP(bid, datetime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
         GROUP BY
             bid.auction,
             window_start,
             window_end
     ) AS AuctionBids
         JOIN (
    SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime_c,
        CountBids.endtime_c
    FROM (
             SELECT
                 count(*) AS num,
                 window_start AS starttime_c,
                 window_end AS endtime_c
             FROM
                 HOP(bid, datetime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
             GROUP BY
                 bid.auction,
                 window_start,
                 window_end
         ) AS CountBids
    GROUP BY
        CountBids.starttime_c, CountBids.endtime_c
) AS MaxBids
              ON AuctionBids.starttime = MaxBids.starttime_c AND
                 AuctionBids.endtime = MaxBids.endtime_c AND
                 AuctionBids.num >= MaxBids.maxn
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q7 AS
SELECT B.auction,
       B.price,
       B.bidder,
       B.datetime,
       B.extra
from bid B
         JOIN (SELECT MAX(price) AS maxprice,
                      window_end as datetime
               FROM
                   TUMBLE(bid, datetime, INTERVAL '10' SECOND)
               GROUP BY window_start, window_end) B1
              ON B.price = B1.maxprice
WHERE B.datetime BETWEEN B1.datetime - INTERVAL '10' SECOND AND B1.datetime
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q8 AS
SELECT P.id,
       P.name,
       P.starttime
FROM (SELECT id,
             name,
             window_start AS starttime,
             window_end   AS endtime
      FROM TUMBLE(person, datetime, INTERVAL '10' SECOND)
      GROUP BY id,
               name,
               window_start,
               window_end) P
         JOIN (SELECT seller,
                      window_start AS starttime,
                      window_end   AS endtime
               FROM TUMBLE(auction, datetime, INTERVAL '10' SECOND)
               GROUP BY seller,
                        window_start,
                        window_end) A
              ON P.id = A.seller
                  AND P.starttime = A.starttime
                  AND P.endtime = A.endtime
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q9 AS
SELECT id,
       itemname,
       description,
       initialbid,
       reserve,
       datetime,
       expires,
       seller,
       category,
       extra,
       auction,
       bidder,
       price,
       bid_datetime,
       bid_extra
FROM (SELECT A.*,
             B.auction,
             B.bidder,
             B.price,
             B.datetime                                                                  AS bid_datetime,
             B.extra AS bid_extra,
             ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.datetime ASC) AS rownum
      FROM auction A,
           bid B
      WHERE A.id = B.auction AND B.datetime BETWEEN A.datetime AND A.expires) tmp
WHERE rownum <= 1
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q12 AS
SELECT bidder, count(*) as bid_count, window_start, window_end
FROM TUMBLE(bid, p_time, INTERVAL '10' SECOND)
GROUP BY bidder, window_start, window_end
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE FUNCTION count_char(varchar, varchar) RETURNS bigint
    LANGUAGE python AS count_char USING LINK 'http://localhost:8815'; -- Replace the address 'localhost' with your host ip.
CREATE SINK nexmark_q14 AS
SELECT auction,
       bidder,
       0.908 * price as price,
       CASE
           WHEN
                       extract(hour from datetime) >= 8 AND
                       extract(hour from datetime) <= 18
               THEN 'dayTime'
           WHEN
                       extract(hour from datetime) <= 6 OR
                       extract(hour from datetime) >= 20
               THEN 'nightTime'
           ELSE 'otherTime'
           END       AS bidTimeType,
       datetime,
       extra,
       count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000
  AND 0.908 * price < 50000000
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q15 AS
SELECT to_char(datetime, 'YYYY-MM-DD')                                          as "day",
       count(*)                                                                  AS total_bids,
       count(*) filter (where price < 10000)                                     AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000)                AS rank2_bids,
        count(*) filter (where price >= 1000000)                                  AS rank3_bids,
        count(distinct bidder)                                                    AS total_bidders,
       count(distinct bidder) filter (where price < 10000)                       AS rank1_bidders,
        count(distinct bidder) filter (where price >= 10000 and price < 1000000)  AS rank2_bidders,
        count(distinct bidder) filter (where price >= 1000000)                    AS rank3_bidders,
        count(distinct auction)                                                   AS total_auctions,
       count(distinct auction) filter (where price < 10000)                      AS rank1_auctions,
        count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
        count(distinct auction) filter (where price >= 1000000)                   AS rank3_auctions
FROM bid
GROUP BY to_char(datetime, 'YYYY-MM-DD')
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q16 AS
SELECT channel,
       to_char(datetime, 'YYYY-MM-DD')                                          as "day",
       max(to_char(datetime, 'HH:mm'))                                          as "minute",
       count(*)                                                                  AS total_bids,
       count(*) filter (where price < 10000)                                     AS rank1_bids,
        count(*) filter (where price >= 10000 and price < 1000000)                AS rank2_bids,
        count(*) filter (where price >= 1000000)                                  AS rank3_bids,
        count(distinct bidder)                                                    AS total_bidders,
       count(distinct bidder) filter (where price < 10000)                       AS rank1_bidders,
        count(distinct bidder) filter (where price >= 10000 and price < 1000000)  AS rank2_bidders,
        count(distinct bidder) filter (where price >= 1000000)                    AS rank3_bidders,
        count(distinct auction)                                                   AS total_auctions,
       count(distinct auction) filter (where price < 10000)                      AS rank1_auctions,
        count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
        count(distinct auction) filter (where price >= 1000000)                   AS rank3_auctions
FROM bid
GROUP BY channel, to_char(datetime, 'YYYY-MM-DD')
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q17 AS
SELECT auction,
       to_char(datetime, 'YYYY-MM-DD')                           AS day,
           count(*)                                                   AS total_bids,
           count(*) filter (where price < 10000)                      AS rank1_bids,
           count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
           count(*) filter (where price >= 1000000)                   AS rank3_bids,
           min(price)                                                 AS min_price,
           max(price)                                                 AS max_price,
           avg(price)                                                 AS avg_price,
           sum(price)                                                 AS sum_price
FROM bid
GROUP BY auction, to_char(datetime, 'YYYY-MM-DD')
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q18 AS
SELECT auction, bidder, price, channel, url, datetime, extra
FROM (SELECT *,
             ROW_NUMBER() OVER (
                     PARTITION BY bidder, auction
                     ORDER BY datetime DESC
                     ) AS rank_number
      FROM bid)
WHERE rank_number <= 1
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q19 AS
SELECT auction, bidder, price, channel, url, datetime, extra
FROM (SELECT *,
             ROW_NUMBER() OVER (
                     PARTITION BY auction
                     ORDER BY price DESC
                 ) AS rank_number
      FROM bid)
WHERE rank_number <= 10
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q20 AS
SELECT auction,
       bidder,
       price,
       channel,
       url,
       B.datetime as bid_datetime,
       B.extra     as bid_extra,
       itemname,
       description,
       initialbid,
       reserve,
       A.datetime as auction_datetime,
       expires,
       seller,
       category,
       A.extra     as auction_extra
FROM bid AS B
         INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q21 AS
SELECT auction,
       bidder,
       price,
       channel,
       CASE
           WHEN LOWER(channel) = 'apple' THEN '0'
           WHEN LOWER(channel) = 'google' THEN '1'
           WHEN LOWER(channel) = 'facebook' THEN '2'
           WHEN LOWER(channel) = 'baidu' THEN '3'
           ELSE (regexp_match(url, '(&|^)channel_id=([^&]*)'))[2]
           END
           AS channel_id
FROM bid
WHERE (regexp_match(url, '(&|^)channel_id=([^&]*)'))[2] is not null
       or LOWER(channel) in ('apple', 'google', 'facebook', 'baidu')
WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q22 AS
SELECT auction,
       bidder,
       price,
       channel,
       split_part(url, '/', 3) as dir1,
       split_part(url, '/', 4) as dir2,
       split_part(url, '/', 5) as dir3
FROM bid
    WITH ( connector = 'blackhole', type = 'append-only');


CREATE SINK nexmark_q5_rewrite AS
SELECT
    B.auction,
    B.num
FROM (
         SELECT
             auction,
             num,
             /*use rank here to express top-N with ties*/
             rank() over (partition by starttime order by num desc) as num_rank
         FROM (
                  SELECT bid.auction, count(*) AS num, window_start AS starttime
                  FROM HOP(bid, datetime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
                  GROUP BY window_start, window_end, bid.auction
              )
     ) B
WHERE num_rank <= 1
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');


CREATE SINK nexmark_q7_rewrite AS
SELECT
    B.auction,
    B.price,
    B.bidder,
    B.datetime
FROM (
         SELECT
             auction,
             price,
             bidder,
             datetime,
             /*use rank here to express top-N with ties*/
             rank() over (partition by window_end order by price desc) as price_rank
         FROM
             TUMBLE(bid, datetime, INTERVAL '10' SECOND)
     ) B
WHERE price_rank <= 1
    WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');