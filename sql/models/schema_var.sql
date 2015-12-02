-- Schema creation
CREATE SCHEMA IF NOT EXISTS "var";

-- We will keep all different scada models
DROP TABLE IF EXISTS "var"."model";
CREATE TABLE "var"."model" (
"id" int2 NOT NULL,
"name" varchar,
"comments" varchar,
CONSTRAINT "model_pkey" PRIMARY KEY ("id")
);


-- Data related to every service (location) available
DROP TABLE IF EXISTS "var"."service";
CREATE TABLE "var"."service" (
"id" int4 NOT NULL,
"code" varchar,
"name" varchar,
"status" varchar,
"model_id" int2,
CONSTRAINT "service_pkey" PRIMARY KEY ("id"),
CONSTRAINT "service_model_id_fkey" FOREIGN KEY ("model_id") REFERENCES "var"."model" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);


-- We will create a separate table for every service: sensor_1, sensor_2... sensor_n
-- Each table will have a different structure depending his scada model
DROP TABLE IF EXISTS "var"."sensor_1";
CREATE TABLE "var"."sensor_1" (
"id" int4 NOT NULL,
"line_kind" int2,
"line_wos_add" varchar(255),
"line_title" varchar(255),
CONSTRAINT "sensor_1_pkey" PRIMARY KEY ("id")
);


DROP TABLE IF EXISTS "var"."sensor_2";
CREATE TABLE "var"."sensor_2" (
"id" int4 NOT NULL,
"label" varchar(255),
"number_in_station" int4,
"unit" varchar(255),
"value" numeric(14,2),
"station_id" int2,
"status_id" varchar(50),
CONSTRAINT "sensor_2_pkey" PRIMARY KEY ("id")
)

DROP TABLE IF EXISTS "var"."sensor_2_logical";
CREATE TABLE "var"."sensor_2_logical" (
"id" int4 NOT NULL,
"label" varchar(255),
"number_in_station" int4,
"category" int2,
"alarm_id" int4,
"state_0" varchar,
"state_1" varchar,
"value" int2,
"station_id" int2,
"status_id" varchar(50),
CONSTRAINT "sensor_2_logical_pkey" PRIMARY KEY ("id")
);


-- We will create a separate table for every service: scada_1, scada_2... scada_n
-- In each table we will save data of all the sensors of this service
DROP TABLE IF EXISTS "var"."scada_1";
CREATE TABLE "var"."scada_1" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
"tstamp" timestamp DEFAULT now(),
CONSTRAINT "scada_1_pkey" PRIMARY KEY ("id")
);

DROP TABLE IF EXISTS "var"."scada_2";
CREATE TABLE "var"."scada_2" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
"tstamp" timestamp DEFAULT now(),
CONSTRAINT "scada_2_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "var"."scada_2_logical" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
"tstamp" timestamp(6) DEFAULT now(),
CONSTRAINT "scada_2_logical_pkey" PRIMARY KEY ("id"),
);


DROP TABLE IF EXISTS "var"."scada_n";
CREATE TABLE "var"."scada_n" (
"id" SERIAL,
"sensor_id" int4 NOT NULL,
"step_date" int8 NOT NULL,
"step_value" numeric(10,2) NOT NULL,
"tstamp" timestamp DEFAULT now(),
CONSTRAINT "scada_n_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "var"."status" (
"id" varchar NOT NULL,
CONSTRAINT "status_pkey" PRIMARY KEY ("id")
)

-- Alter SQL instructions
ALTER TABLE "var"."service" ADD FOREIGN KEY ("scada_model_id") REFERENCES "var"."scada_model" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "var"."scada_1" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "var"."scada_2" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "var"."scada_2_logical" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor_2_logical" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "var"."scada_n" ADD FOREIGN KEY ("sensor_id") REFERENCES "var"."sensor" ("id") ON DELETE RESTRICT ON UPDATE CASCADE;



