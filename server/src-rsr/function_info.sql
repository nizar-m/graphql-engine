SELECT
  row_to_json (
    (
      SELECT
        e
        FROM
            (
              SELECT
                hf.has_variadic,
                hf.function_type,
                hf.return_type_schema,
                hf.return_type_name,
                hf.return_type_type,
                hf.returns_set,
                hf.input_arg_names,
                (
                  select json_agg(row_to_json(x)) from
                    ( select
                      fo.oid as oid,
                      case
                        when arr_elem_ty.oid is NOT NULL then 1
                        else 0
                      end as dimension
                      from
		      (select unnest( COALESCE(pp.proallargtypes, pp.proargtypes) :: int[]) as oid )  fo
                      left outer join pg_type arr_elem_ty on arr_elem_ty.typarray = fo.oid
                    ) x
                )  as input_arg_types,
                exists(
                  SELECT
                    1
                    FROM
                        information_schema.tables
                   WHERE
                table_schema = hf.return_type_schema
                AND table_name = hf.return_type_name
                ) AS returns_table
            ) AS e
    )
  ) AS "raw_function_info"
  FROM
      hdb_catalog.hdb_function_agg hf left outer join pg_proc pp
      on hf.function_name = pp.proname and hf.function_schema = pp.pronamespace::regnamespace::text
 WHERE
  hf.function_schema = $1
  AND hf.function_name = $2
