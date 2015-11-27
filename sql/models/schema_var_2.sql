-- Schema creation scada models of type PCWin
CREATE SCHEMA IF NOT EXISTS "var_2";


-- We will keep in 1 table data of all the sensors of all scadas
CREATE TABLE "var_2"."sensor" (
"id" int4 NOT NULL,
"line_kind" int2,
"line_wos_add" var_2char(255),
"line_title" var_2char(255),
"scada_id" int4,
CONSTRAINT "line_pkey" PRIMARY KEY ("id")
);


-- We will create a separate table for every scada station: scada_1, scada_2... scada_n
-- In each table we will save data of all the sensors of this scada station
DROP TABLE IF EXISTS "var_2"."scada_1";
CREATE TABLE "var_2"."scada_1" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
CONSTRAINT "scada_1_pkey" PRIMARY KEY ("id")
);


-- Alter SQL instructions
ALTER TABLE "var_2"."scada" ADD FOREIGN KEY ("scada_model_id") REFERENCES "var_2"."scada_model" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var_2"."scada_1" ADD FOREIGN KEY ("sensor_id") REFERENCES "var_2"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var_2"."scada_n" ADD FOREIGN KEY ("sensor_id") REFERENCES "var_2"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "var_2"."sensor" ADD FOREIGN KEY ("scada_id") REFERENCES "var_2"."scada" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;


-- 20151119
ALTER TABLE "var_2"."scada_1" ADD COLUMN "tstamp" timestamp DEFAULT now();
ALTER TABLE "var_2"."scada_n" ADD COLUMN "tstamp" timestamp DEFAULT now();
CREATE INDEX  ON "var_2"."scada_1" ("sensor_id");



