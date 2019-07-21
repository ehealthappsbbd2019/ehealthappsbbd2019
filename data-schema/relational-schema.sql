CREATE TABLE doctor (
    id integer NOT NULL,
	name character varying,
    address character varying,
    date_of_birth timestamp without time zone,
    specialty character varying
);

CREATE TABLE laboratory_worker (
    id integer NOT NULL,
	name character varying,
    address character varying,
    date_of_birth timestamp without time zone,
    occupation character varying
);

CREATE TABLE patient (
    id integer NOT NULL,
	name character varying,
    address character varying,
    date_of_birth timestamp without time zone,
    gender character(1)
);

ALTER TABLE ONLY doctor
    ADD CONSTRAINT pk_id_doctor PRIMARY KEY (id);

ALTER TABLE ONLY laboratory_worker
    ADD CONSTRAINT pk_id_laboratory_worker PRIMARY KEY (id);

ALTER TABLE ONLY patient
    ADD CONSTRAINT pk_patient_id PRIMARY KEY (id);
