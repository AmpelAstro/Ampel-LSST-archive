-- BEGIN;

-- /* 2023-10-17 UT: add forced photometry */

-- CREATE TABLE fp_hist (
-- 	fp_hist_id SERIAL NOT NULL,
--     _objectid INTEGER NOT NULL,
--     field INTEGER,
--     rcid tinyint,
--     fid tinyint,
--     pid BIGINT NOT NULL,
--     rfid BIGINT NOT NULL,
--     sciinpseeing FLOAT,
--     scibckgnd FLOAT,
--     scisigpix FLOAT,
--     magzpsci FLOAT,
--     magzpsciunc FLOAT,
--     magzpscirms FLOAT,
--     clrcoeff FLOAT,
--     clrcounc FLOAT,
--     exptime FLOAT,
--     adpctdif1 FLOAT,
--     adpctdif2 FLOAT,
--     diffmaglim FLOAT,
--     programid tinyint NOT NULL,
--     jd DOUBLE PRECISION NOT NULL,
--     forcediffimflux FLOAT,
--     forcediffimfluxunc FLOAT,
--     procstatus STRING,
--     distnr FLOAT,
--     ranr FLOAT,
--     decnr FLOAT,
--     magnr FLOAT,
--     sigmanr FLOAT,
--     sharpnr FLOAT,
-- 	PRIMARY KEY (fp_hist_id), 
--     -- FIXME how to uniquely identify these? (processing id, ZTF name)?
-- 	UNIQUE (_objectid, pid)
-- );

-- CREATE TABLE alert_fp_hist_pivot (
-- 	alert_id INTEGER NOT NULL, 
-- 	fp_hist_id INTEGER[] NOT NULL, 
-- 	PRIMARY KEY (alert_id), 
-- 	FOREIGN KEY(alert_id) REFERENCES alert (alert_id) ON DELETE CASCADE ON UPDATE CASCADE
-- )

-- SELECT cron.schedule('0 23 * * *', $$begin; SELECT compactify_pivot_table('fp_hist'); commit;$$);

-- -- NB: already inserted by hand
-- -- INSERT INTO versions (alert_version) VALUES (4.02);

-- COMMIT;
