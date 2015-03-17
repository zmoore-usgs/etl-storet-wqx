show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'validate dw start time: ' || systimestamp from dual;

begin
  declare old_rows    int;
          new_rows    int;
          pass_fail   varchar2(15);
          end_job     boolean := false;

begin
	
	dbms_output.put_line('validating...');

	dbms_output.put_line('... pc_result');
	select count(*) into old_rows from pc_result partition (pc_result_storet);
	select count(*) into new_rows from pc_result_swap_storet;
	if new_rows > 110000000 and new_rows > old_rows - 10000000 then
    	pass_fail := 'PASS';
    else
        pass_fail := 'FAIL';
    	end_job := true;
        $IF $$empty_db $THEN
            pass_fail := 'PASS empty_db';
            end_job := false;
        $END
    end if;
    dbms_output.put_line(pass_fail || ': table comparison for pc_result: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999')));

    dbms_output.put_line('... station');
    select count(*) into old_rows from station partition (station_storet);
    select count(*) into new_rows from station_swap_storet;
    if new_rows > 500000 and new_rows > old_rows - 50000 then
        pass_fail := 'PASS';
    else
        pass_fail := 'FAIL';
    	end_job := true;
        $IF $$empty_db $THEN
            pass_fail := 'PASS empty_db';
            end_job := false;
        $END
    end if;
    dbms_output.put_line(pass_fail || ': table comparison for station: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999')));

    dbms_output.put_line('... qw_portal_summary');
    select count(*) into old_rows from qwportal_summary partition (summary_storet);
    select count(*) into new_rows from qwportal_summary_swap_storet;
    if new_rows > 8500 and new_rows > old_rows - 1000 then
        pass_fail := 'PASS';
    else
        pass_fail := 'FAIL';
    	end_job := true;
        $IF $$empty_db $THEN
            pass_fail := 'PASS empty_db';
            end_job := false;
        $END
    end if;
    dbms_output.put_line(pass_fail || ': table comparison for qwportal_summary: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999')));

  	if end_job then
    	raise_application_error(-20666, 'Failed to pass one or more validation checks.');
  	end if;

end;
end;
/

select 'validate dw tables end time: ' || systimestamp from dual;
