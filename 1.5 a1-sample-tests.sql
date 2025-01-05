--------------------------------------
-- Sample Tests for INFS2200/7903   --
-- Assignment 1, 2024 (v1.2 Sep-02) --
--                                  --
-- BEFORE YOU RUN THIS FILE:        --
--   1. Run a1-2024-librarydb.sql   --
--      NOTE: you need v1.2         --
--   2. Run 4xxxxxxx.sql            --
--                                  --
-- You must do this every time,     -- 
--   as many tests insert data.     --
--                                  --
-- THESE ARE NOT ALL OF THE TESTS.  --
-- Some of the real tests will test --
-- for false positives / negatives. --
--------------------------------------

\set ECHO none
\set QUIET on
\set client_min_messages ERROR
\pset pager off

\prompt 'By continuing, you confirm that you are running this with the v1.2 sample data (press Enter to continue) ' _

-- We test with only one trigger enabled at a time, so we must take care to
--   ensure that all assignment triggers can be disabled by name.
SELECT trigger_name as "MISNAMED TRIGGERS (if any)", event_object_table as "Table"
	from information_schema.triggers 
    where lower(event_object_table) in ('events', 'items', 'patrons', 'works')
    and lower(trigger_name) not in ('ai_missing_return', 'bi_holds', 'bi_loss_charge', 
        'bi_item_id', 'bi_email_addr', 'bi_guardian');

with t_names as (select * from (values 
    ('ai_missing_return'), ('bi_holds'), ('bi_loss_charge'), 
        ('bi_item_id'), ('bi_email_addr'), ('bi_guardian')) as t (t_name))
SELECT t_name as "MISSING TRIGGERS (if any)"
	from t_names left join information_schema.triggers 
    on (t_name = lower(trigger_name))
    where trigger_name is null;

\prompt 'Please fix any misnamed triggers and note any missing ones. ' _

\echo 'Attempting to disable all triggers here, any mis-named or missing ones will cause an error message...'
alter table EVENTS disable trigger AI_MISSING_RETURN;
alter table EVENTS disable trigger BI_HOLDS;
alter table EVENTS disable trigger BI_LOSS_CHARGE;
alter table ITEMS disable trigger BI_ITEM_ID;
alter table PATRONS disable trigger BI_EMAIL_ADDR;
alter table PATRONS disable trigger BI_GUARDIAN;

-- We can also test for any missing/misnamed functions
with f_names as (select * from (values 
    ('udf_bi_guardian'), ('udf_bi_email_addr'), ('udf_bi_item_id'), 
    ('udf_bi_loss_charge'), ('udf_ai_missing_return'), ('udf_bi_holds')) as t (f_name))
select f_name as "MISSING/MISNAMED FUNCTIONS (if any)" 
    from f_names left join pg_catalog.pg_proc
    on (f_name = lower(proname))
    where proname is null;

create temp table TESTS (question varchar, item varchar, UNIQUE(question, item));

\echo '\nIf all goes well, there will be no error messages under any of the question headings.'
\echo '(Note that failure may occur even without an error, e.g. if a value or behaviour is incorrect.)'

\echo '\n==================== Q1 ====================\n'

do $$
begin
	begin
		-- should block
        insert into events (patron_id, item_id, event_type, time_stamp) values
            (1, 'UQ01000000001', 'Found', '2024-08-01');
	exception
		when check_violation then
			insert into TESTS VALUES ('1', 'CK_EVENT_TYPE block');
		when others then
			raise notice 'CK_EVENT_TYPE unexpected error';
	end;
end;
$$ language plpgsql;

\echo '\n==================== Q2 ====================\n'
alter table PATRONS enable trigger BI_GUARDIAN;

do $$ begin
begin
-- Incorrect child (null guardian)
insert into patrons (patron_name, email_address, dob, guardian) 
    values ('Pugsley Addams', 'pugsley.addams@example.com', '2015-09-29', null);
exception when raise_exception then end;
end $$ language plpgsql;

alter table PATRONS disable trigger BI_GUARDIAN;

insert into TESTS select '2.1', 'BI_GUARDIAN child null' from Patrons
    where not exists (select 1 from Patrons where patron_name ~~ 'Pugsley%')
limit 1;


alter table PATRONS enable trigger BI_EMAIL_ADDR;

do $$ begin
begin
-- Incorrect adult (blank email)
insert into patrons (patron_name, email_address, dob, guardian) 
    values ('Gomez Addams', '', '1930-03-30', null);
exception when raise_exception then end;
end $$ language plpgsql;

alter table PATRONS disable trigger BI_EMAIL_ADDR;

insert into TESTS select '2.2', 'BI_EMAIL_ADDRESS adult blank' from Patrons
    where not exists (select 1 from Patrons where patron_name ~~ 'Gomez%')
limit 1;

\echo '\n==================== Q3 ====================\n'

-- No sample test for Q3.1

-- Q3.2 

-- Let's just make sure you're using the sequence correctly
do $$ begin PERFORM setval('ITEM_ID_SEQ', 9876000032, false); end $$ language plpgsql;

alter table ITEMS enable trigger BI_ITEM_ID;
insert into ITEMS (isbn) values ('1000000040959');
alter table ITEMS disable trigger BI_ITEM_ID;

insert into TESTS select '3.2', 'BI_ITEM_ID skip'
    from ITEMS where isbn = '1000000040959' and item_id = 'UQ98760000325' limit 1;

-- Just putting this back to normal
do $$ begin PERFORM setval('ITEM_ID_SEQ', 1000000002, false); end $$ language plpgsql;


-- No sample test for Q3.3

\echo '\n==================== Q4 ====================\n'

-- Q4.1 

alter table EVENTS enable trigger BI_LOSS_CHARGE;
insert into EVENTS (patron_id, item_id, event_type, time_stamp) values (70, 'UQ01000040140', 'Loss', '2024-08-01 16:45:52+00');
alter table EVENTS disable trigger BI_LOSS_CHARGE;


insert into TESTS select '4.1', 'BI_LOSS_CHARGE' from Events 
    where 
    patron_id = 70
    and item_id = 'UQ01000040140'
    and event_type = 'Loss'
    and time_stamp = '2024-08-01 16:45:52+00'
    and charge = 1088;


-- Q4.2 

alter table EVENTS enable trigger AI_MISSING_RETURN;
insert into EVENTS (patron_id, item_id, event_type, time_stamp) values (1, 'UQ01000040139', 'Loan', '2024-08-02 09:21:45+00');
alter table EVENTS disable trigger AI_MISSING_RETURN;

insert into TESTS select '4.2', 'AI_MISSING_RETURN'
    from Events 
    where 
        exists (select * from Events where item_id = 'UQ01000040139' and event_type = 'Return' and time_stamp = '2024-08-02 08:21:45+00' and patron_id = 70)
        and exists (select * from Events where item_id = 'UQ01000040139' and event_type = 'Loan' and time_stamp = '2024-08-02 09:21:45+00' and patron_id = 1)
        and 1 = (select count(*) from Events where item_id = 'UQ01000040139' and event_type = 'Return' and time_stamp > '2024-08-01')
    limit 1;

-- Q4.3

alter table EVENTS enable trigger BI_HOLDS;

-- Can we place a hold at all?
insert into EVENTS (patron_id, item_id, event_type, time_stamp) values (1, 'UQ01000031993', 'Hold', '2024-08-03 10:01:00+00');

-- What if it's lost?
do $$ begin
begin
insert into EVENTS (patron_id, item_id, event_type, time_stamp) values (1, 'UQ01000006344', 'Hold', '2024-08-03 10:02:00+00');
exception when raise_exception then 
    insert into TESTS select '4.3', 'BI_HOLDS lost item'
        from Events 
            where not exists (select * from Events where item_id = 'UQ01000006344' and time_stamp > '2024-05-31')
        limit 1;
end;
end $$ language plpgsql;

-- Or on loan to someone else?
insert into EVENTS (patron_id, item_id, event_type, time_stamp) values (1, 'UQ01000040128', 'Hold', '2024-08-03 10:03:00+00');

alter table EVENTS disable trigger BI_HOLDS;

-- Check dates on success cases
insert into TESTS select '4.3', 'BI_HOLDS base case'
    from Events 
    where 
        exists (select * from Events where item_id = 'UQ01000031993' and event_type = 'Hold' and time_stamp = '2024-08-17 10:01:00+00' and patron_id = 1)
        and 1 = (select count(*) from Events where item_id = 'UQ01000031993')
    limit 1;

insert into TESTS select '4.3', 'BI_HOLDS +42'
    from Events 
    where 
        exists (select * from Events where item_id = 'UQ01000040128' and event_type = 'Hold' and time_stamp = '2024-07-11 16:29:28+00' and patron_id = 1)
        -- This is actually a bit of a bizarre edge case in that the hold has already expired, but it *does* test that you're doing the right thing
        and 1 = (select count(*) from Events where item_id = 'UQ01000040128' and time_stamp > '2024-05-31')
    limit 1;



\echo '\n==================== Results ====================\n'

\echo 'Successful tests:'

select * from TESTS;

\echo 'Failed tests:'

with all_items as (select * from (VALUES
    ('1', 'CK_EVENT_TYPE block'),
    ('2.1', 'BI_GUARDIAN child null'),
    ('2.2', 'BI_EMAIL_ADDRESS adult blank'),
    ('3.2', 'BI_ITEM_ID skip'),
    ('4.1', 'BI_LOSS_CHARGE'),
    ('4.2', 'AI_MISSING_RETURN'),
    ('4.3', 'BI_HOLDS base case'),
    ('4.3', 'BI_HOLDS lost item'),
    ('4.3', 'BI_HOLDS +42')) as t (question, item))
select question, item from all_items
    where item not in (select item from TESTS)
order by question;

drop table TESTS;