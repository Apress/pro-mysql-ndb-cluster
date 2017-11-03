DROP PROCEDURE IF EXISTS sigcount;
delimiter //
CREATE PROCEDURE sigcount(t INT)
  BEGIN
    DROP TEMPORARY TABLE IF EXISTS tmpstat;
    CREATE TEMPORARY TABLE tmpstat ENGINE MEMORY
        SELECT * FROM ndbinfo.threadstat;
    SELECT SLEEP(t) FROM DUAL;

    SELECT STRAIGHT_JOIN
        s2.node_id,
        s2.thr_no,
        s2.thr_nm,
        (s2.os_now - s1.os_now) AS time_ms,
        (s2.c_loop - s1.c_loop) AS loops,
        (s2.c_exec - s1.c_exec) AS execs,
        (s2.c_wait - s1.c_wait) AS waits,
        (s2.c_exec - s1.c_exec) / (s2.c_loop - s1.c_loop) AS signals_per_loop
      FROM
        tmpstat s1 INNER JOIN
        ndbinfo.threadstat s2 USING (node_id, thr_no)
      ORDER BY
        s1.node_id, s1.thr_no;
    
    DROP TEMPORARY TABLE tmpstat;
END;//
delimiter ;
