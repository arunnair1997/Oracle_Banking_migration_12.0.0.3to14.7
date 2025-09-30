-- PROCEDURE PROC_DUMP_USER_OBJECTS_DDL (ARUNN_ADMIN)

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ARUNN_ADMIN"."PROC_DUMP_USER_OBJECTS_DDL" (
  p_directory_name        IN VARCHAR2,
  p_add_slash_for_plsql  IN VARCHAR2 DEFAULT 'Y'  -- adds "/" after PL/SQL objs
) AS
  ---------------------------------------------------------------------------
  -- Declarations
  ---------------------------------------------------------------------------
  v_schema       VARCHAR2(128) := USER;
  v_errlog_fh    UTL_FILE.FILE_TYPE;
  v_errlog_name  VARCHAR2(256);
  v_dir_ok       PLS_INTEGER;
 
  v_fh           UTL_FILE.FILE_TYPE;
  v_file_name    VARCHAR2(256);
  v_md_type      VARCHAR2(128);
  v_ddl          CLOB;
 
  ---------------------------------------------------------------------------
  -- Helpers
  ---------------------------------------------------------------------------
  PROCEDURE put_clob(p_fh UTL_FILE.FILE_TYPE, p_c CLOB) IS
    pos   PLS_INTEGER := 1;
    len   PLS_INTEGER := NVL(DBMS_LOB.GETLENGTH(p_c), 0);
    chunk CONSTANT PLS_INTEGER := 32767;
    buf   VARCHAR2(32767);
  BEGIN
    WHILE pos <= len LOOP
      buf := DBMS_LOB.SUBSTR(p_c, LEAST(chunk, len - pos + 1), pos);
      UTL_FILE.PUT(p_fh, buf);
      pos := pos + chunk;
    END LOOP;
    UTL_FILE.NEW_LINE(p_fh);
  END;
 
  FUNCTION sanitize_filename(p IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    -- keep original case of object_name; replace unsafe chars with "_"
    RETURN REGEXP_REPLACE(p, '[^A-Za-z0-9_-]', '_');
  END;
 
  FUNCTION extension_for_type(p_obj_type IN VARCHAR2) RETURN VARCHAR2 IS
    t VARCHAR2(128) := UPPER(p_obj_type);
  BEGIN
    CASE t
      WHEN 'INDEX'         THEN RETURN '.idx';
      WHEN 'PACKAGE BODY'  THEN RETURN '.sql';
      WHEN 'PROCEDURE'     THEN RETURN '.prc';
      WHEN 'PACKAGE'       THEN RETURN '.spc';
      WHEN 'FUNCTION'      THEN RETURN '.fnc';
      WHEN 'SEQUENCE'      THEN RETURN '.seq';
      WHEN 'TABLE'         THEN RETURN '.ddl';
      WHEN 'VIEW'          THEN RETURN '.vw';
      ELSE RETURN NULL; -- skip all other types
    END CASE;
  END;
 
  FUNCTION map_to_md_type(p_obj_type IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    -- DBMS_METADATA expects names like PACKAGE_BODY, not "PACKAGE BODY"
    RETURN REPLACE(UPPER(p_obj_type), ' ', '_');
  END;
 
  FUNCTION is_plsql_type(p_obj_type IN VARCHAR2) RETURN BOOLEAN IS
    t VARCHAR2(128) := UPPER(p_obj_type);
  BEGIN
    RETURN t IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY');
  END;
BEGIN
  v_errlog_name := 'DDL_DUMP_ERRORS_'||LOWER(v_schema)||'.log';
 
  -- Verify DIRECTORY exists
  SELECT COUNT(*) INTO v_dir_ok
  FROM ALL_DIRECTORIES
  WHERE DIRECTORY_NAME = UPPER(p_directory_name);
  IF v_dir_ok = 0 THEN
    RAISE_APPLICATION_ERROR(-20000,
      'Directory '||p_directory_name||' not found. Ask DBA to CREATE DIRECTORY and GRANT READ,WRITE.');
  END IF;
 
  -- DBMS_METADATA formatting
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);
 
  -- Open error log
  v_errlog_fh := UTL_FILE.FOPEN(p_directory_name, v_errlog_name, 'w', 32767);
  UTL_FILE.PUT_LINE(v_errlog_fh, '-- Errors while dumping DDL for '||v_schema||' at '
                                 ||TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD HH24:MI:SS.FF3'));
 
  -- Loop all objects of the allowed types (DB links are not in USER_OBJECTS anyway)
  FOR o IN (
    SELECT object_name, object_type
    FROM   USER_OBJECTS
    WHERE  UPPER(object_type) IN ('INDEX','PACKAGE BODY','PROCEDURE','PACKAGE','FUNCTION','SEQUENCE','TABLE','VIEW')
    ORDER  BY object_type, object_name
  ) LOOP
    DECLARE
      v_ext VARCHAR2(10);
    BEGIN
      v_ext := extension_for_type(o.object_type);
      IF v_ext IS NULL THEN
        CONTINUE; -- skip unsupported types
      END IF;
 
      v_md_type   := map_to_md_type(o.object_type);
      v_file_name := sanitize_filename(o.object_name) || v_ext;
 
      -- Fetch DDL
      BEGIN
        v_ddl := DBMS_METADATA.GET_DDL(v_md_type, o.object_name, v_schema);
      EXCEPTION
        WHEN OTHERS THEN
          UTL_FILE.PUT_LINE(v_errlog_fh,
            '['||o.object_type||'] '||o.object_name||' -> GET_DDL error: '||SUBSTR(SQLERRM,1,3500));
          CONTINUE;
      END;
 
      -- Write per-object file
      BEGIN
        v_fh := UTL_FILE.FOPEN(p_directory_name, v_file_name, 'w', 32767);
        -- optional tiny header
        UTL_FILE.PUT_LINE(v_fh, '-- '||o.object_type||' '||o.object_name||' ('||v_schema||')');
        put_clob(v_fh, v_ddl);
 
        IF p_add_slash_for_plsql = 'Y' AND is_plsql_type(o.object_type) THEN
          UTL_FILE.PUT_LINE(v_fh, '/');
        END IF;
 
        UTL_FILE.FCLOSE(v_fh);
      EXCEPTION
        WHEN OTHERS THEN
          IF UTL_FILE.IS_OPEN(v_fh) THEN UTL_FILE.FCLOSE(v_fh); END IF;
          UTL_FILE.PUT_LINE(v_errlog_fh,
            '['||o.object_type||'] '||o.object_name||' -> write error: '||SUBSTR(SQLERRM,1,3500));
          CONTINUE;
      END;
    END;
  END LOOP;
 
  UTL_FILE.PUT_LINE(v_errlog_fh, '-- End of run.');
  UTL_FILE.FCLOSE(v_errlog_fh);
 
EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line ('Bombed due to ' ||SQLERRM);
    BEGIN IF UTL_FILE.IS_OPEN(v_fh) THEN UTL_FILE.FCLOSE(v_fh); END IF; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN IF UTL_FILE.IS_OPEN(v_errlog_fh) THEN UTL_FILE.FCLOSE(v_errlog_fh); END IF; EXCEPTION WHEN OTHERS THEN NULL; END;
    RAISE;
END;
/
/
