-- Schema creation
CREATE SCHEMA IF NOT EXISTS "audit";

DROP TABLE IF EXISTS "audit"."log_detail";
CREATE TABLE "audit"."log_detail" (
"id" SERIAL,
"service_id" int4,
"sensor_id" int4,
"first_date" int8 NOT NULL,
"last_date" int8 NOT NULL,
"rec_number" int4,
"addr" text,
"tstamp" timestamp(6) DEFAULT now() NOT NULL,
"query" text,
CONSTRAINT "log_detail_pkey" PRIMARY KEY ("id")
);

ALTER TABLE "audit"."log_detail" OWNER TO "gisadmin";

COMMENT ON TABLE "audit"."log_detail" IS 'History of inserts made by our Python function that copies info from SCADA Databases into this PostGIS Database';

COMMENT ON COLUMN "audit"."log_detail"."id" IS 'Unique identifier for each transaction event';

COMMENT ON COLUMN "audit"."log_detail"."service_id" IS 'Service identifier';

COMMENT ON COLUMN "audit"."log_detail"."sensor_id" IS 'Sensor identifier (from selected scada station)';

COMMENT ON COLUMN "audit"."log_detail"."first_date" IS 'Date of the first record inserted in this transaction';

COMMENT ON COLUMN "audit"."log_detail"."last_date" IS 'Date of the last record inserted in this transaction';

COMMENT ON COLUMN "audit"."log_detail"."rec_number" IS 'Number of records inserted in this transaction';

COMMENT ON COLUMN "audit"."log_detail"."addr" IS 'IP address of client that issued query';

COMMENT ON COLUMN "audit"."log_detail"."tstamp" IS 'Timestamp for tx in which audited event occurred';

COMMENT ON COLUMN "audit"."log_detail"."query" IS 'Queries (inserts) related with this transaction. May be more than one statement';