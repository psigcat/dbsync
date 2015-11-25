-- Schema creation
CREATE SCHEMA IF NOT EXISTS "var";

-- We will keep all different scada models
DROP TABLE IF EXISTS "var"."scada_model";
CREATE TABLE "var"."scada_model" (
"id" int2 NOT NULL,
"name" varchar,
"comments" varchar,
CONSTRAINT "model_pkey" PRIMARY KEY ("id")
);


-- Data related to every scada station available
DROP TABLE IF EXISTS "var"."scada";
CREATE TABLE "var"."scada" (
"id" int4 NOT NULL,
"name" varchar,
"location" varchar,
"status" varchar,
"scada_model_id" int2,
CONSTRAINT "sensor_pkey" PRIMARY KEY ("id")
);


-- We will keep in 1 table data of all the sensors of all scadas
CREATE TABLE "var"."sensor" (
"id" int4 NOT NULL,
"line_kind" int2,
"line_wos_add" varchar(255),
"line_title" varchar(255),
"scada_id" int4,
CONSTRAINT "line_pkey" PRIMARY KEY ("id")
);


-- We will create a separate table for every scada station: scada_1, scada_2... scada_n
-- In each table we will save data of all the sensors of this scada station
DROP TABLE IF EXISTS "var"."scada_1";
CREATE TABLE "var"."scada_1" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
CONSTRAINT "scada_1_pkey" PRIMARY KEY ("id")
);

DROP TABLE IF EXISTS "var"."scada_n";
CREATE TABLE "var"."scada_n" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
CONSTRAINT "scada_n_pkey" PRIMARY KEY ("id")
);


-- Alter SQL instructions
ALTER TABLE "var"."scada" ADD FOREIGN KEY ("scada_model_id") REFERENCES "var"."scada_model" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var"."scada_1" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var"."scada_n" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var"."sensor" ADD FOREIGN KEY ("scada_id") REFERENCES "var"."scada" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;


-- 20151119
ALTER TABLE "var"."scada_1" ADD COLUMN "tstamp" timestamp DEFAULT now();
ALTER TABLE "var"."scada_n" ADD COLUMN "tstamp" timestamp DEFAULT now();
CREATE INDEX  ON "var"."scada_1" ("sensor_id");



