-- Q1 CHECK Constraint (1 mark)
ALTER TABLE Events
ADD CONSTRAINT CK_EVENT_TYPE
CHECK (event_type IN ('Loan', 'Return', 'Hold', 'Loss'));


-- Q2.1 Constraints with triggers (2 marks)
CREATE OR REPLACE FUNCTION UDF_BI_GUARDIAN()
RETURNS TRIGGER AS $guardian$
BEGIN
	IF (NEW.dob > CURRENT_DATE - INTERVAL '18 years') THEN
		IF NEW.guardian IS NULL THEN
			RAISE EXCEPTION 'Patrons under 18 years of age require a guardian.';
		END IF;

		IF NOT EXISTS (SELECT 1 FROM Patrons WHERE patron_id = NEW.guardian AND dob
		<= CURRENT_DATE - INTERVAL '18 years') THEN
			RAISE EXCEPTION 'The specified guardian must be a registered patron and at least
			18 years of age.';
		END IF;
	END IF;
	RETURN NEW;
END;
$guardian$ LANGUAGE plpgsql;

CREATE TRIGGER BI_GUARDIAN
BEFORE INSERT ON Patrons
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_GUARDIAN();


-- Q2.2 Constraints with triggers (2 marks)
CREATE OR REPLACE FUNCTION UDF_BI_EMAIL_ADDR()
RETURNS TRIGGER AS $email$
BEGIN
	IF (NEW.dob <= CURRENT_DATE - INTERVAL '18 years') THEN
		IF NEW.email_address IS NULL OR TRIM(NEW.email_address = '') THEN
			RAISE EXCEPTION 'Patrons 18 years or older must provide an email address.';
		END IF;
	ELSE
		IF NEW.email_address IS NOT NULL THEN
			RAISE EXCEPTION 'Patrons under 18 years old must not provide an email address.';
		END IF;
	END IF;
	RETURN NEW;
END;
$email$ LANGUAGE plpgsql;

CREATE TRIGGER BI_EMAIL_ADDR
BEFORE INSERT ON Patrons
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_EMAIL_ADDR();


-- Q3.1 Sequence creation (1 mark)
CREATE SEQUENCE ITEM_ID_SEQ
START WITH 1000000000
INCREMENT BY 1
MINVALUE 1000000000
MAXVALUE 9999999999
NO CYCLE;


-- Q3.2 Sequences and Triggers (2 marks)
CREATE OR REPLACE FUNCTION UDF_BI_ITEM_ID()
RETURNS TRIGGER AS $item$
DECLARE
	seq_num BIGINT;
	checksum INT;
BEGIN
	seq_num := nextval('ITEM_ID_SEQ');
	
	checksum := 0;
	FOR i IN 1..10 LOOP
		checksum := checksum + substring(seq_num::text, i, 1)::int;
	END LOOP;
	checksum := checksum % 10;

	NEW.item_id := 'UQ' || seq_num::text || checksum::text;
	RETURN NEW;
END;
$item$ LANGUAGE plpgsql;

CREATE TRIGGER BI_ITEM_ID
BEFORE INSERT ON Items
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_ITEM_ID();


-- Q3.3 Sequence identification (1 mark)
SELECT
	t.relname AS "Table Name",
	seq.relname AS "Sequence Name",
	ns.nspname AS "Schema"
FROM
	pg_class seq
	INNER JOIN pg_depend dep ON seq.oid = dep.objid
	INNER JOIN pg_class t ON dep.refobjid = t.oid
	INNER JOIN pg_namespace ns ON seq.relnamespace = ns.oid
WHERE
	seq.relkind = 'S'
	AND dep.deptype = 'a'
	AND t.relkind = 'r'
ORDER BY
	"Schema",
	"Table Name",
	"Sequence Name";

-- Q4.1 Losses (2 marks)
CREATE OR REPLACE FUNCTION UDF_BI_LOSS_CHARGE() 
RETURNS TRIGGER AS $loss$
BEGIN
	IF NEW.event_type = 'Loss' THEN
		SELECT W.cost INTO NEW.charge
		FROM Works W
		JOIN Items I ON W.isbn = I.isbn
		WHERE I.item_id = NEW.item_id;
	END IF;
	RETURN NEW;
END;
$loss$ LANGUAGE plpgsql;

CREATE TRIGGER BI_LOSS_CHARGE
BEFORE INSERT ON Events
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_LOSS_CHARGE(); 


-- Q4.2 Missing Returns (4 marks)
CREATE OR REPLACE FUNCTION UDF_AI_MISSING_RETURN()
RETURNS TRIGGER AS $loan$
DECLARE
	last_event RECORD;
BEGIN
	SELECT * INTO last_event
	FROM Events
	WHERE item_id = NEW.item_id	
	AND time_stamp < NEW.time_stamp
	ORDER BY time_stamp DESC
	LIMIT 1;

	IF last_event.event_type = 'Loan' THEN
		IF NEW.time_stamp <= last_event.time_stamp + INTERVAL '1 hour 1 millisecond' THEN
			RAISE EXCEPTION 'New loan cannot be within 1 hour and 1 millisecond of the last
			loan for the same item.';
		END IF;

		INSERT INTO Events (patron_id, item_id, event_type, time_stamp, charge)
		VALUES (last_event.patron_id, last_event.item_id, 'Return', NEW.time_stamp - INTERVAL '1 hour', NULL);
	END IF;
	RETURN NEW;
END;
$loan$ LANGUAGE plpgsql;

CREATE TRIGGER AI_MISSING_RETURN
AFTER INSERT ON Events
FOR EACH ROW
WHEN (NEW.event_type = 'Loan')
EXECUTE FUNCTION UDF_AI_MISSING_RETURN();


-- Q4.3 Holds (5 marks)
CREATE OR REPLACE FUNCTION UDF_BI_HOLDS()
RETURNS TRIGGER AS $$
DECLARE
    last_loan_time TIMESTAMP;
    last_hold_time TIMESTAMP;
    is_item_on_loan BOOLEAN;
    can_item_be_held BOOLEAN;
    item_is_lost BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM Events
        WHERE item_id = NEW.item_id
        AND event_type = 'Loss'
        AND time_stamp <= NEW.time_stamp
    ) INTO item_is_lost;

    IF item_is_lost THEN
        RAISE EXCEPTION 'A Hold cannot be placed on a lost item with ID %', NEW.item_id;
    END IF;

    SELECT time_stamp INTO last_loan_time
    FROM Events
    WHERE event_type = 'Loan' AND item_id = NEW.item_id
    ORDER BY time_stamp DESC
    LIMIT 1;

    SELECT time_stamp INTO last_hold_time
    FROM Events
    WHERE event_type = 'Hold' AND item_id = NEW.item_id
    ORDER BY time_stamp DESC
    LIMIT 1;

    is_item_on_loan := (
        SELECT EXISTS (
            SELECT 1
            FROM Events
            WHERE event_type = 'Loan' AND item_id = NEW.item_id
            AND NOT EXISTS (
                SELECT 1 FROM Events
                WHERE event_type = 'Return' AND item_id = NEW.item_id
                AND time_stamp > last_loan_time
            )
        )
    );

    can_item_be_held := NOT is_item_on_loan;

    IF NEW.event_type = 'Hold' THEN
        IF last_hold_time IS NOT NULL AND (last_loan_time IS NULL OR last_hold_time > last_loan_time) THEN
            RAISE EXCEPTION 'Consecutive holds are not permitted without a prior loan or return event.';
        END IF;

        IF NOT (can_item_be_held OR is_item_on_loan) THEN
            RAISE EXCEPTION 'Cannot place hold: Item is neither available nor on loan.';
        END IF;

        IF is_item_on_loan THEN
            NEW.time_stamp := last_loan_time + INTERVAL '42 days';
        ELSE
            NEW.time_stamp := NEW.time_stamp + INTERVAL '14 days';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER BI_HOLDS
BEFORE INSERT ON Events
FOR EACH ROW
WHEN (NEW.event_type = 'Hold')
EXECUTE FUNCTION UDF_BI_HOLDS();