-- Schema creation
CREATE SCHEMA IF NOT EXISTS "audit";

DROP TABLE IF EXISTS "audit"."log_detail";
CREATE TABLE "audit"."log_detail" (
"id" SERIAL,
"tstamp" timestamp(0) DEFAULT now() NOT NULL,
"service_id" int4,
"sensor_id" int4,
"sensor_type" int2,
"first_date" timestamp(0) NOT NULL,
"last_date" timestamp(0) NOT NULL,
"rec_found" int4,
"rec_inserted" int4,
CONSTRAINT "log_detail_pkey" PRIMARY KEY ("id")
);

COMMENT ON TABLE "audit"."log_detail" IS 'History of inserts made by our Python script that copies info from SCADA Databases into this PostGIS Database';
COMMENT ON COLUMN "audit"."log_detail"."id" IS 'Unique identifier for each transaction event';
COMMENT ON COLUMN "audit"."log_detail"."tstamp" IS 'Timestamp for tx in which audited event occurred';
COMMENT ON COLUMN "audit"."log_detail"."service_id" IS 'Service identifier';
COMMENT ON COLUMN "audit"."log_detail"."sensor_id" IS 'Sensor identifier (from selected service location)';
COMMENT ON COLUMN "audit"."log_detail"."sensor_type" IS 'Sensor type. 0: general - 1: logical';
COMMENT ON COLUMN "audit"."log_detail"."first_date" IS 'Date of the first record inserted in this transaction';
COMMENT ON COLUMN "audit"."log_detail"."last_date" IS 'Date of the last record inserted in this transaction';
COMMENT ON COLUMN "audit"."log_detail"."rec_found" IS 'Number of records found in this transaction';
COMMENT ON COLUMN "audit"."log_detail"."rec_inserted" IS 'Number of records inserted in this transaction';

GRANT ALL ON ALL TABLES IN SCHEMA audit TO sincro;
GRANT ALL ON ALL SEQUENCES IN SCHEMA audit TO sincro;