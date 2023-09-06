WITH table_data (
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, 
    col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, 
    col21, col22, col23, col24, col25, col26, col27, col28, col29, col30
    ) as (
        SELECT * FROM YOUR_TABLE_NAME_HERE
        )
, metadata as (
    SELECT distinct
    table_name
    , column_name
    , ordinal_position
    , data_type
    FROM information_schema.columns
    WHERE table_schema='default'
    AND table_name = 'YOUR_TABLE_NAME_HERE'
    ORDER BY ordinal_position
)
, metadata2 as (
    SELECT m.*
    , concat('col', cast(m.ordinal_position as varchar(3))) temp_column_name
    FROM metadata m
)
, col11_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col11) total_nonnull_count
            , count(distinct col11) distinct_count
            , min(length(col11)) min_charlength
            , max(length(col11)) max_charlength
            , min(col11) alphabet_min
            , max(col11) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col11) null_count
                FROM table_data
                WHERE col11 is null or col11 LIKE '' or col11 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) has_numbers_count
                FROM table_data
                WHERE regexp_like(col11, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) has_letters_count
                FROM table_data
                WHERE regexp_like(col11, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) has_decimals_count
                FROM table_data
                WHERE regexp_like(col11, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col11, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col11, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col11) date_count
                FROM table_data
                WHERE regexp_like(col11, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col11
                    , CASE 
                    WHEN regexp_like(col11, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col11, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col11, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col11, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col11, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col11, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col11, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col11, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col11, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col11, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col11, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col11 as int))) numeric_max
            , min(try(cast(col11 as int))) numeric_min
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col11 as decimal(18,2)))) decimal_max
            , min(try(cast(col11 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col11,'/',3),'-',split_part(col11,'/',1),'-',split_part(col11,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col11,'/',3),'-',split_part(col11,'/',1),'-',split_part(col11,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col11,'/',1),'-',split_part(col11,'/',2),'-',split_part(col11,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col11,'/',1),'-',split_part(col11,'/',2),'-',split_part(col11,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col11,'-',3),'-',split_part(col11,'-',1),'-',split_part(col11,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col11,'-',3),'-',split_part(col11,'-',1),'-',split_part(col11,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col11
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col11
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col11 as int))) max_int
                , min(try(cast(col11 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col11, 1, 4),'-',substr(col11, 5, 2),'-', substr(col11, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col11, 1, 4),'-',substr(col11, 5, 2),'-', substr(col11, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col11 is not null and col11 <> '' and col11 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col11 as date))) max_date6
            , min(try(cast(col11 as date))) min_date6
            FROM table_data
            WHERE col11 is not null and col11 <> '' and col11 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col11'
)
, col12_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col12) total_nonnull_count
            , count(distinct col12) distinct_count
            , min(length(col12)) min_charlength
            , max(length(col12)) max_charlength
            , min(col12) alphabet_min
            , max(col12) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col12) null_count
                FROM table_data
                WHERE col12 is null or col12 LIKE '' or col12 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) has_numbers_count
                FROM table_data
                WHERE regexp_like(col12, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) has_letters_count
                FROM table_data
                WHERE regexp_like(col12, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) has_decimals_count
                FROM table_data
                WHERE regexp_like(col12, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col12, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col12, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col12) date_count
                FROM table_data
                WHERE regexp_like(col12, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col12
                    , CASE 
                    WHEN regexp_like(col12, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col12, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col12, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col12, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col12, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col12, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col12, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col12, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col12, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col12, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col12, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col12 as int))) numeric_max
            , min(try(cast(col12 as int))) numeric_min
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col12 as decimal(18,2)))) decimal_max
            , min(try(cast(col12 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col12,'/',3),'-',split_part(col12,'/',1),'-',split_part(col12,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col12,'/',3),'-',split_part(col12,'/',1),'-',split_part(col12,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col12,'/',1),'-',split_part(col12,'/',2),'-',split_part(col12,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col12,'/',1),'-',split_part(col12,'/',2),'-',split_part(col12,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col12,'-',3),'-',split_part(col12,'-',1),'-',split_part(col12,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col12,'-',3),'-',split_part(col12,'-',1),'-',split_part(col12,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col12
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col12
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col12 as int))) max_int
                , min(try(cast(col12 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col12, 1, 4),'-',substr(col12, 5, 2),'-', substr(col12, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col12, 1, 4),'-',substr(col12, 5, 2),'-', substr(col12, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col12 is not null and col12 <> '' and col12 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col12 as date))) max_date6
            , min(try(cast(col12 as date))) min_date6
            FROM table_data
            WHERE col12 is not null and col12 <> '' and col12 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col12'
)
, col13_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col13) total_nonnull_count
            , count(distinct col13) distinct_count
            , min(length(col13)) min_charlength
            , max(length(col13)) max_charlength
            , min(col13) alphabet_min
            , max(col13) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col13) null_count
                FROM table_data
                WHERE col13 is null or col13 LIKE '' or col13 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) has_numbers_count
                FROM table_data
                WHERE regexp_like(col13, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) has_letters_count
                FROM table_data
                WHERE regexp_like(col13, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) has_decimals_count
                FROM table_data
                WHERE regexp_like(col13, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col13, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col13, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col13) date_count
                FROM table_data
                WHERE regexp_like(col13, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col13
                    , CASE 
                    WHEN regexp_like(col13, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col13, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col13, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col13, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col13, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col13, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col13, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col13, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col13, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col13, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col13, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col13 as int))) numeric_max
            , min(try(cast(col13 as int))) numeric_min
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col13 as decimal(18,2)))) decimal_max
            , min(try(cast(col13 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col13,'/',3),'-',split_part(col13,'/',1),'-',split_part(col13,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col13,'/',3),'-',split_part(col13,'/',1),'-',split_part(col13,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col13,'/',1),'-',split_part(col13,'/',2),'-',split_part(col13,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col13,'/',1),'-',split_part(col13,'/',2),'-',split_part(col13,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col13,'-',3),'-',split_part(col13,'-',1),'-',split_part(col13,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col13,'-',3),'-',split_part(col13,'-',1),'-',split_part(col13,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col13
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col13
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col13 as int))) max_int
                , min(try(cast(col13 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col13, 1, 4),'-',substr(col13, 5, 2),'-', substr(col13, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col13, 1, 4),'-',substr(col13, 5, 2),'-', substr(col13, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col13 is not null and col13 <> '' and col13 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col13 as date))) max_date6
            , min(try(cast(col13 as date))) min_date6
            FROM table_data
            WHERE col13 is not null and col13 <> '' and col13 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col13'
)
, col14_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col14) total_nonnull_count
            , count(distinct col14) distinct_count
            , min(length(col14)) min_charlength
            , max(length(col14)) max_charlength
            , min(col14) alphabet_min
            , max(col14) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col14) null_count
                FROM table_data
                WHERE col14 is null or col14 LIKE '' or col14 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) has_numbers_count
                FROM table_data
                WHERE regexp_like(col14, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) has_letters_count
                FROM table_data
                WHERE regexp_like(col14, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) has_decimals_count
                FROM table_data
                WHERE regexp_like(col14, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col14, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col14, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col14) date_count
                FROM table_data
                WHERE regexp_like(col14, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col14
                    , CASE 
                    WHEN regexp_like(col14, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col14, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col14, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col14, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col14, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col14, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col14, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col14, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col14, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col14, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col14, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col14 as int))) numeric_max
            , min(try(cast(col14 as int))) numeric_min
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col14 as decimal(18,2)))) decimal_max
            , min(try(cast(col14 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col14,'/',3),'-',split_part(col14,'/',1),'-',split_part(col14,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col14,'/',3),'-',split_part(col14,'/',1),'-',split_part(col14,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col14,'/',1),'-',split_part(col14,'/',2),'-',split_part(col14,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col14,'/',1),'-',split_part(col14,'/',2),'-',split_part(col14,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col14,'-',3),'-',split_part(col14,'-',1),'-',split_part(col14,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col14,'-',3),'-',split_part(col14,'-',1),'-',split_part(col14,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col14
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col14
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col14 as int))) max_int
                , min(try(cast(col14 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col14, 1, 4),'-',substr(col14, 5, 2),'-', substr(col14, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col14, 1, 4),'-',substr(col14, 5, 2),'-', substr(col14, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col14 is not null and col14 <> '' and col14 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col14 as date))) max_date6
            , min(try(cast(col14 as date))) min_date6
            FROM table_data
            WHERE col14 is not null and col14 <> '' and col14 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col14'
)
, col15_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col15) total_nonnull_count
            , count(distinct col15) distinct_count
            , min(length(col15)) min_charlength
            , max(length(col15)) max_charlength
            , min(col15) alphabet_min
            , max(col15) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col15) null_count
                FROM table_data
                WHERE col15 is null or col15 LIKE '' or col15 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) has_numbers_count
                FROM table_data
                WHERE regexp_like(col15, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) has_letters_count
                FROM table_data
                WHERE regexp_like(col15, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) has_decimals_count
                FROM table_data
                WHERE regexp_like(col15, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col15, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col15, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col15) date_count
                FROM table_data
                WHERE regexp_like(col15, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col15
                    , CASE 
                    WHEN regexp_like(col15, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col15, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col15, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col15, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col15, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col15, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col15, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col15, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col15, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col15, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col15, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col15 as int))) numeric_max
            , min(try(cast(col15 as int))) numeric_min
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col15 as decimal(18,2)))) decimal_max
            , min(try(cast(col15 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col15,'/',3),'-',split_part(col15,'/',1),'-',split_part(col15,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col15,'/',3),'-',split_part(col15,'/',1),'-',split_part(col15,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col15,'/',1),'-',split_part(col15,'/',2),'-',split_part(col15,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col15,'/',1),'-',split_part(col15,'/',2),'-',split_part(col15,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col15,'-',3),'-',split_part(col15,'-',1),'-',split_part(col15,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col15,'-',3),'-',split_part(col15,'-',1),'-',split_part(col15,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col15
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col15
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col15 as int))) max_int
                , min(try(cast(col15 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col15, 1, 4),'-',substr(col15, 5, 2),'-', substr(col15, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col15, 1, 4),'-',substr(col15, 5, 2),'-', substr(col15, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col15 is not null and col15 <> '' and col15 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col15 as date))) max_date6
            , min(try(cast(col15 as date))) min_date6
            FROM table_data
            WHERE col15 is not null and col15 <> '' and col15 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col15'
)
, col16_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col16) total_nonnull_count
            , count(distinct col16) distinct_count
            , min(length(col16)) min_charlength
            , max(length(col16)) max_charlength
            , min(col16) alphabet_min
            , max(col16) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col16) null_count
                FROM table_data
                WHERE col16 is null or col16 LIKE '' or col16 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) has_numbers_count
                FROM table_data
                WHERE regexp_like(col16, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) has_letters_count
                FROM table_data
                WHERE regexp_like(col16, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) has_decimals_count
                FROM table_data
                WHERE regexp_like(col16, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col16, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col16, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col16) date_count
                FROM table_data
                WHERE regexp_like(col16, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col16
                    , CASE 
                    WHEN regexp_like(col16, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col16, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col16, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col16, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col16, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col16, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col16, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col16, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col16, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col16, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col16, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col16 as int))) numeric_max
            , min(try(cast(col16 as int))) numeric_min
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col16 as decimal(18,2)))) decimal_max
            , min(try(cast(col16 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col16,'/',3),'-',split_part(col16,'/',1),'-',split_part(col16,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col16,'/',3),'-',split_part(col16,'/',1),'-',split_part(col16,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col16,'/',1),'-',split_part(col16,'/',2),'-',split_part(col16,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col16,'/',1),'-',split_part(col16,'/',2),'-',split_part(col16,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col16,'-',3),'-',split_part(col16,'-',1),'-',split_part(col16,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col16,'-',3),'-',split_part(col16,'-',1),'-',split_part(col16,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col16
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col16
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col16 as int))) max_int
                , min(try(cast(col16 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col16, 1, 4),'-',substr(col16, 5, 2),'-', substr(col16, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col16, 1, 4),'-',substr(col16, 5, 2),'-', substr(col16, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col16 is not null and col16 <> '' and col16 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col16 as date))) max_date6
            , min(try(cast(col16 as date))) min_date6
            FROM table_data
            WHERE col16 is not null and col16 <> '' and col16 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col16'
)
, col17_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col17) total_nonnull_count
            , count(distinct col17) distinct_count
            , min(length(col17)) min_charlength
            , max(length(col17)) max_charlength
            , min(col17) alphabet_min
            , max(col17) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col17) null_count
                FROM table_data
                WHERE col17 is null or col17 LIKE '' or col17 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) has_numbers_count
                FROM table_data
                WHERE regexp_like(col17, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) has_letters_count
                FROM table_data
                WHERE regexp_like(col17, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) has_decimals_count
                FROM table_data
                WHERE regexp_like(col17, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col17, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col17, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col17) date_count
                FROM table_data
                WHERE regexp_like(col17, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col17
                    , CASE 
                    WHEN regexp_like(col17, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col17, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col17, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col17, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col17, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col17, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col17, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col17, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col17, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col17, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col17, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col17 as int))) numeric_max
            , min(try(cast(col17 as int))) numeric_min
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col17 as decimal(18,2)))) decimal_max
            , min(try(cast(col17 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col17,'/',3),'-',split_part(col17,'/',1),'-',split_part(col17,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col17,'/',3),'-',split_part(col17,'/',1),'-',split_part(col17,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col17,'/',1),'-',split_part(col17,'/',2),'-',split_part(col17,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col17,'/',1),'-',split_part(col17,'/',2),'-',split_part(col17,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col17,'-',3),'-',split_part(col17,'-',1),'-',split_part(col17,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col17,'-',3),'-',split_part(col17,'-',1),'-',split_part(col17,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col17
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col17
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col17 as int))) max_int
                , min(try(cast(col17 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col17, 1, 4),'-',substr(col17, 5, 2),'-', substr(col17, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col17, 1, 4),'-',substr(col17, 5, 2),'-', substr(col17, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col17 is not null and col17 <> '' and col17 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col17 as date))) max_date6
            , min(try(cast(col17 as date))) min_date6
            FROM table_data
            WHERE col17 is not null and col17 <> '' and col17 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col17'
)
, col18_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col18) total_nonnull_count
            , count(distinct col18) distinct_count
            , min(length(col18)) min_charlength
            , max(length(col18)) max_charlength
            , min(col18) alphabet_min
            , max(col18) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col18) null_count
                FROM table_data
                WHERE col18 is null or col18 LIKE '' or col18 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) has_numbers_count
                FROM table_data
                WHERE regexp_like(col18, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) has_letters_count
                FROM table_data
                WHERE regexp_like(col18, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) has_decimals_count
                FROM table_data
                WHERE regexp_like(col18, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col18, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col18, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col18) date_count
                FROM table_data
                WHERE regexp_like(col18, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col18
                    , CASE 
                    WHEN regexp_like(col18, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col18, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col18, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col18, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col18, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col18, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col18, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col18, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col18, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col18, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col18, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col18 as int))) numeric_max
            , min(try(cast(col18 as int))) numeric_min
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col18 as decimal(18,2)))) decimal_max
            , min(try(cast(col18 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col18,'/',3),'-',split_part(col18,'/',1),'-',split_part(col18,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col18,'/',3),'-',split_part(col18,'/',1),'-',split_part(col18,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col18,'/',1),'-',split_part(col18,'/',2),'-',split_part(col18,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col18,'/',1),'-',split_part(col18,'/',2),'-',split_part(col18,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col18,'-',3),'-',split_part(col18,'-',1),'-',split_part(col18,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col18,'-',3),'-',split_part(col18,'-',1),'-',split_part(col18,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col18
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col18
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col18 as int))) max_int
                , min(try(cast(col18 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col18, 1, 4),'-',substr(col18, 5, 2),'-', substr(col18, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col18, 1, 4),'-',substr(col18, 5, 2),'-', substr(col18, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col18 is not null and col18 <> '' and col18 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col18 as date))) max_date6
            , min(try(cast(col18 as date))) min_date6
            FROM table_data
            WHERE col18 is not null and col18 <> '' and col18 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col18'
)
, col19_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col19) total_nonnull_count
            , count(distinct col19) distinct_count
            , min(length(col19)) min_charlength
            , max(length(col19)) max_charlength
            , min(col19) alphabet_min
            , max(col19) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col19) null_count
                FROM table_data
                WHERE col19 is null or col19 LIKE '' or col19 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) has_numbers_count
                FROM table_data
                WHERE regexp_like(col19, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) has_letters_count
                FROM table_data
                WHERE regexp_like(col19, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) has_decimals_count
                FROM table_data
                WHERE regexp_like(col19, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col19, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col19, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col19) date_count
                FROM table_data
                WHERE regexp_like(col19, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col19
                    , CASE 
                    WHEN regexp_like(col19, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col19, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col19, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col19, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col19, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col19, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col19, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col19, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col19, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col19, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col19, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col19 as int))) numeric_max
            , min(try(cast(col19 as int))) numeric_min
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col19 as decimal(18,2)))) decimal_max
            , min(try(cast(col19 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col19,'/',3),'-',split_part(col19,'/',1),'-',split_part(col19,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col19,'/',3),'-',split_part(col19,'/',1),'-',split_part(col19,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col19,'/',1),'-',split_part(col19,'/',2),'-',split_part(col19,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col19,'/',1),'-',split_part(col19,'/',2),'-',split_part(col19,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col19,'-',3),'-',split_part(col19,'-',1),'-',split_part(col19,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col19,'-',3),'-',split_part(col19,'-',1),'-',split_part(col19,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col19
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col19
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col19 as int))) max_int
                , min(try(cast(col19 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col19, 1, 4),'-',substr(col19, 5, 2),'-', substr(col19, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col19, 1, 4),'-',substr(col19, 5, 2),'-', substr(col19, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col19 is not null and col19 <> '' and col19 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col19 as date))) max_date6
            , min(try(cast(col19 as date))) min_date6
            FROM table_data
            WHERE col19 is not null and col19 <> '' and col19 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col19'
)
, col20_str (
    SELECT
    md.table_name, md.column_name, md.ordinal_position, me.data_type, md.temp_column_name
    , null_count, total_nonnull_count, distinct_count, contains_duplicates, contains_non_alphanumerics
    , contains_letters, min_charlength, max_charlength, alphabet_min, alphabet_max
    , contains_numbers, contains_only_numbers
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_min else 'N/A' END numeric_min
    , CASE WHEN contains_only_numbers = 'Y' THEN numeric_max else 'N/A' END numeric_max
    , contains_decimals, decimal_datatype_flag
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_min else 'n/A' END decimal_min
    , CASE WHEN decimal_datatype_flag = 'Y' THEN decimal_max else 'n/A' END decimal_max
    , CASE WHEN date_flag <> 'N/A' THEN 'Y' ELSE 'N' END date_flag
    , CASE WHEN date_flag <> 'N/A' THEN date_flag ELSE 'N/A' END date_form
    , CASE 
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN max_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN max_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN max_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN max_date4
        WHEN date_flag = 'yyyymmdd' THEN max_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN max_date6
        ELSE 'N/A' END AS max_date
    , CASE
        WHEN date_flag = 'mm/dd/yyyy' OR date_flag = 'm/d/yyyy' THEN min_date1
        WHEN date_flag = 'yyyy/mm/dd' OR date_flag = 'yyyy/m/d' THEN min_date2
        WHEN date_flag = 'mm-dd-yyyy' OR date_flag = 'm-d-yyyy' THEN min_date3
        WHEN date_flag = 'yyyy-mm-dd' OR date_flag = 'yyyy-m-d' THEN min_date4
        WHEN date_flag = 'yyyymmdd' THEN min_date5
        WHEN date_flag = 'yyyy-mm-dd hh:mm:ss.mmm ZON' OR date_flag = 'yyyy-mm-dd hh:mm:ss.mmm' THEN min_date6
        ELSE 'N/A' END AS min_date
    FROM (
        SELECT 
        a.null_count, a.total_nonnull_count, a.distinct_count
        , CASE WHEN a.total_nonnull_count = a.distinct_count THEN 'N' ELSE 'Y' END contains_duplicates
        , CASE WHEN a.has_non_alphanumeric_characters_count = 0 THEN 'N' ELSE 'Y' END contains_non_alphanumerics

        , CASE WHEN a.has_letters_count = 0 THEN 'N' ELSE 'Y' END contains_letters
        , a.min_charlength, a.max_charlength, a.alphabet_min, a.alphabet_max

        , CASE WHEN a.has_numbers_count = 0 THEN 'N' ELSE 'Y' END contains_numbers
        , CASE WHEN a.has_numbers_count > 0 AND a.has_letters_count = 0 THEN 'Y' ELSE 'N' END contains_only_numbers
        , CASE WHEN b.numeric_max is not null then cast(b.numeric_max as varchar) ELSE 'N/A' END numeric_max
        , CASE WHEN b.numeric_min is not null then cast(b.numeric_min as varchar) ELSE 'N/A' END numeric_min

        , CASE WHEN a.has_decimals_count = 0 THEN 'N' ELSE 'Y' END contains_decimals
        , CASE WHEN a.decimal_datatype_count = a.distinct_count then 'Y' ELSE 'N' END decimal_datatype_flag
        , CASE WHEN c.decimal_max is not null THEN cast(c.decimal_max as varchar) ELSE 'N/A' END decimal_max
        , CASE WHEN c.decimal_min is not null THEN cast(c.decimal_min as varchar) ELSE 'N/A' END decimal_min

        , CASE WHEN a.dateformat <> 'N/A' AND a.date_count = a.total_nonnull_count THEN a.dateformat ELSE 'N/A' END date_flag
        , CASE WHEN d.max_date1 IS NOT NULL THEN CAST(d.max_date1 as varchar) ELSE 'N/A' END max_date1
        , CASE WHEN d.min_date1 IS NOT NULL THEN CAST(d.min_date1 as varchar) ELSE 'N/A' END min_date1
        , CASE WHEN e.max_date2 IS NOT NULL THEN CAST(e.max_date2 as varchar) ELSE 'N/A' END max_date2
        , CASE WHEN e.min_date2 IS NOT NULL THEN CAST(e.min_date2 as varchar) ELSE 'N/A' END min_date2
        , CASE WHEN f.max_date3 IS NOT NULL THEN CAST(f.max_date3 as varchar) ELSE 'N/A' END max_date3
        , CASE WHEN f.min_date3 IS NOT NULL THEN CAST(f.min_date3 as varchar) ELSE 'N/A' END min_date3
        , CASE WHEN g.max_date4 IS NOT NULL THEN CAST(g.max_date4 as varchar) ELSE 'N/A' END max_date4
        , CASE WHEN g.min_date4 IS NOT NULL THEN CAST(g.min_date4 as varchar) ELSE 'N/A' END min_date4
        , CASE WHEN h.max_date5 IS NOT NULL THEN CAST(h.max_date5 as varchar) ELSE 'N/A' END max_date5
        , CASE WHEN h.min_date5 IS NOT NULL THEN CAST(h.min_date5 as varchar) ELSE 'N/A' END min_date5
        , CASE WHEN i.max_date6 IS NOT NULL THEN CAST(i.max_date6 as varchar) ELSE 'N/A' END max_date6
        , CASE WHEN i.min_date6 IS NOT NULL THEN CAST(i.min_date6 as varchar) ELSE 'N/A' END min_date6
        FROM (
            SELECT
            a.null_count null_count
            , count(col20) total_nonnull_count
            , count(distinct col20) distinct_count
            , min(length(col20)) min_charlength
            , max(length(col20)) max_charlength
            , min(col20) alphabet_min
            , max(col20) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col20) null_count
                FROM table_data
                WHERE col20 is null or col20 LIKE '' or col20 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) has_numbers_count
                FROM table_data
                WHERE regexp_like(col20, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) has_letters_count
                FROM table_data
                WHERE regexp_like(col20, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) has_decimals_count
                FROM table_data
                WHERE regexp_like(col20, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col20, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col20, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col20) date_count
                FROM table_data
                WHERE regexp_like(col20, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
                                        |\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$
                                        |\d{4}\-\d{2}\-\d{2}$
                                        |\d{4}\-\d{1,2}\-\d{1,2}$
                                        |\d{2}\-\d{2}\-\d{4}
                                        |\d{1,2}\-\d{1,2}\-\d{4}
                                        ||\d{4}\/\d{2}\/\d{2}$
                                        |\d{4}\/\d{1,2}\/\d{1,2}$
                                        |\d{2}\/\d{2}\/\d{4}
                                        |\d{1,2}\/\d{1,2}\/\d{4}$
                                        |^\d{8}$'
                                        )=True
            ) g
            ON 1=1
            FULL OUTER JOIN (
                SELECT distinct
                dateformat
                , count(dateformat) format_count
                FROM (
                    SELECT
                    col20
                    , CASE 
                    WHEN regexp_like(col20, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col20, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col20, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col20, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col20, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col20, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col20, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col20, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col20, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col20, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col20, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col20 as int))) numeric_max
            , min(try(cast(col20 as int))) numeric_min
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col20 as decimal(18,2)))) decimal_max
            , min(try(cast(col20 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col20,'/',3),'-',split_part(col20,'/',1),'-',split_part(col20,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col20,'/',3),'-',split_part(col20,'/',1),'-',split_part(col20,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col20,'/',1),'-',split_part(col20,'/',2),'-',split_part(col20,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col20,'/',1),'-',split_part(col20,'/',2),'-',split_part(col20,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col20,'-',3),'-',split_part(col20,'-',1),'-',split_part(col20,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col20,'-',3),'-',split_part(col20,'-',1),'-',split_part(col20,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col20
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col20
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) g
        ON 1=1
        FULL OUTER JOIN (
            SELECT 
            CASE WHEN substr(cast(A.max_int as varchar), 1, 2) in ('19','20') 
                THEN CAST(A.max_date as varchar) 
                ELSE 'N/A' END max_date5
            , CASE WHEN substr(cast(A.min_int as varchar), 1, 2) in ('19','20')
                THEN CAST(A.min_date as varchar)
                ELSE 'N/A' END min_date5
            FROM (
                SELECT 
                max(try(cast(col20 as int))) max_int
                , min(try(cast(col20 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col20, 1, 4),'-',substr(col20, 5, 2),'-', substr(col20, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col20, 1, 4),'-',substr(col20, 5, 2),'-', substr(col20, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col20 is not null and col20 <> '' and col20 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col20 as date))) max_date6
            , min(try(cast(col20 as date))) min_date6
            FROM table_data
            WHERE col20 is not null and col20 <> '' and col20 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col20'
)
, combined as (
SELECT * from col11_str
UNION SELECT * from col12_str
UNION SELECT * FROM col13_str
UNION SELECT * FROM col14_str
UNION SELECT * FROM col15_str
UNION SELECT * FROM col16_str
UNION SELECT * FROM col17_str
UNION SELECT * FROM col18_str
UNION SELECT * FROM col19_str
UNION SELECT * FROM col20_str
)
SELECT * FROM combined ORDER BY ordinal_position;
