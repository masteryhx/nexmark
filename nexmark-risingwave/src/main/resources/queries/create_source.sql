CREATE SOURCE nexmark (
  event_type BIGINT,
  person STRUCT<"id" BIGINT,
                "name" VARCHAR,
                "emailaddress" VARCHAR,
                "creditcard" VARCHAR,
                "city" VARCHAR,
                "state" VARCHAR,
                "datetime" TIMESTAMP,
                "extra" VARCHAR>,
  auction STRUCT<"id" BIGINT,
                "itemname" VARCHAR,
                "description" VARCHAR,
                "initialbid" BIGINT,
                "reserve" BIGINT,
                "datetime" TIMESTAMP,
                "expires" TIMESTAMP,
                "seller" BIGINT,
                "category" BIGINT,
                "extra" VARCHAR>,
  bid STRUCT<"auction" BIGINT,
            "bidder" BIGINT,
            "price" BIGINT,
            "channel" VARCHAR,
            "url" VARCHAR,
            "datetime" TIMESTAMP,
            "extra" VARCHAR>,
  p_time TIMESTAMPTZ as proctime()
) WITH (
  connector = 'kafka',
  topic = 'nexmark-events',
  properties.bootstrap.server = '${KAFKA_HOST}:${KAFKA_PORT}',
  scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE SOURCE nexmark (
  event_type BIGINT,
  person STRUCT<"id" BIGINT,
                "name" VARCHAR,
                "emailaddress" VARCHAR,
                "creditcard" VARCHAR,
                "city" VARCHAR,
                "state" VARCHAR,
                "datetime" TIMESTAMP,
                "extra" VARCHAR>,
  auction STRUCT<"id" BIGINT,
                "itemname" VARCHAR,
                "description" VARCHAR,
                "initialbid" BIGINT,
                "reserve" BIGINT,
                "datetime" TIMESTAMP,
                "expires" TIMESTAMP,
                "seller" BIGINT,
                "category" BIGINT,
                "extra" VARCHAR>,
  bid STRUCT<"auction" BIGINT,
            "bidder" BIGINT,
            "price" BIGINT,
            "channel" VARCHAR,
            "url" VARCHAR,
            "datetime" TIMESTAMP,
            "extra" VARCHAR>,
  p_time TIMESTAMPTZ as proctime()
) WITH (
  connector = 'datagen'
);