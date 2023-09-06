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
, col21_str (
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
            , count(col21) total_nonnull_count
            , count(distinct col21) distinct_count
            , min(length(col21)) min_charlength
            , max(length(col21)) max_charlength
            , min(col21) alphabet_min
            , max(col21) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col21) null_count
                FROM table_data
                WHERE col21 is null or col21 LIKE '' or col21 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) has_numbers_count
                FROM table_data
                WHERE regexp_like(col21, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) has_letters_count
                FROM table_data
                WHERE regexp_like(col21, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) has_decimals_count
                FROM table_data
                WHERE regexp_like(col21, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col21, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col21, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col21) date_count
                FROM table_data
                WHERE regexp_like(col21, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col21
                    , CASE 
                    WHEN regexp_like(col21, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col21, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col21, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col21, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col21, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col21, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col21, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col21, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col21, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col21, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col21, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col21 as int))) numeric_max
            , min(try(cast(col21 as int))) numeric_min
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col21 as decimal(18,2)))) decimal_max
            , min(try(cast(col21 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col21,'/',3),'-',split_part(col21,'/',1),'-',split_part(col21,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col21,'/',3),'-',split_part(col21,'/',1),'-',split_part(col21,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col21,'/',1),'-',split_part(col21,'/',2),'-',split_part(col21,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col21,'/',1),'-',split_part(col21,'/',2),'-',split_part(col21,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col21,'-',3),'-',split_part(col21,'-',1),'-',split_part(col21,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col21,'-',3),'-',split_part(col21,'-',1),'-',split_part(col21,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col21
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col21
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
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
                max(try(cast(col21 as int))) max_int
                , min(try(cast(col21 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col21, 1, 4),'-',substr(col21, 5, 2),'-', substr(col21, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col21, 1, 4),'-',substr(col21, 5, 2),'-', substr(col21, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col21 is not null and col21 <> '' and col21 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col21 as date))) max_date6
            , min(try(cast(col21 as date))) min_date6
            FROM table_data
            WHERE col21 is not null and col21 <> '' and col21 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col21'
)
, col22_str (
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
            , count(col22) total_nonnull_count
            , count(distinct col22) distinct_count
            , min(length(col22)) min_charlength
            , max(length(col22)) max_charlength
            , min(col22) alphabet_min
            , max(col22) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col22) null_count
                FROM table_data
                WHERE col22 is null or col22 LIKE '' or col22 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) has_numbers_count
                FROM table_data
                WHERE regexp_like(col22, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) has_letters_count
                FROM table_data
                WHERE regexp_like(col22, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) has_decimals_count
                FROM table_data
                WHERE regexp_like(col22, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col22, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col22, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col22) date_count
                FROM table_data
                WHERE regexp_like(col22, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col22
                    , CASE 
                    WHEN regexp_like(col22, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col22, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col22, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col22, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col22, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col22, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col22, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col22, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col22, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col22, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col22, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col22 as int))) numeric_max
            , min(try(cast(col22 as int))) numeric_min
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col22 as decimal(18,2)))) decimal_max
            , min(try(cast(col22 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col22,'/',3),'-',split_part(col22,'/',1),'-',split_part(col22,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col22,'/',3),'-',split_part(col22,'/',1),'-',split_part(col22,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col22,'/',1),'-',split_part(col22,'/',2),'-',split_part(col22,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col22,'/',1),'-',split_part(col22,'/',2),'-',split_part(col22,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col22,'-',3),'-',split_part(col22,'-',1),'-',split_part(col22,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col22,'-',3),'-',split_part(col22,'-',1),'-',split_part(col22,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col22
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col22
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
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
                max(try(cast(col22 as int))) max_int
                , min(try(cast(col22 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col22, 1, 4),'-',substr(col22, 5, 2),'-', substr(col22, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col22, 1, 4),'-',substr(col22, 5, 2),'-', substr(col22, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col22 is not null and col22 <> '' and col22 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col22 as date))) max_date6
            , min(try(cast(col22 as date))) min_date6
            FROM table_data
            WHERE col22 is not null and col22 <> '' and col22 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col22'
)
, col23_str (
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
            , count(col23) total_nonnull_count
            , count(distinct col23) distinct_count
            , min(length(col23)) min_charlength
            , max(length(col23)) max_charlength
            , min(col23) alphabet_min
            , max(col23) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col23) null_count
                FROM table_data
                WHERE col23 is null or col23 LIKE '' or col23 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) has_numbers_count
                FROM table_data
                WHERE regexp_like(col23, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) has_letters_count
                FROM table_data
                WHERE regexp_like(col23, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) has_decimals_count
                FROM table_data
                WHERE regexp_like(col23, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col23, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col23, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col23) date_count
                FROM table_data
                WHERE regexp_like(col23, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col23
                    , CASE 
                    WHEN regexp_like(col23, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col23, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col23, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col23, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col23, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col23, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col23, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col23, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col23, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col23, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col23, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col23 as int))) numeric_max
            , min(try(cast(col23 as int))) numeric_min
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col23 as decimal(18,2)))) decimal_max
            , min(try(cast(col23 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col23,'/',3),'-',split_part(col23,'/',1),'-',split_part(col23,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col23,'/',3),'-',split_part(col23,'/',1),'-',split_part(col23,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col23,'/',1),'-',split_part(col23,'/',2),'-',split_part(col23,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col23,'/',1),'-',split_part(col23,'/',2),'-',split_part(col23,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col23,'-',3),'-',split_part(col23,'-',1),'-',split_part(col23,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col23,'-',3),'-',split_part(col23,'-',1),'-',split_part(col23,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col23
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col23
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
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
                max(try(cast(col23 as int))) max_int
                , min(try(cast(col23 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col23, 1, 4),'-',substr(col23, 5, 2),'-', substr(col23, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col23, 1, 4),'-',substr(col23, 5, 2),'-', substr(col23, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col23 is not null and col23 <> '' and col23 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col23 as date))) max_date6
            , min(try(cast(col23 as date))) min_date6
            FROM table_data
            WHERE col23 is not null and col23 <> '' and col23 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col23'
)
, col24_str (
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
            , count(col24) total_nonnull_count
            , count(distinct col24) distinct_count
            , min(length(col24)) min_charlength
            , max(length(col24)) max_charlength
            , min(col24) alphabet_min
            , max(col24) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col24) null_count
                FROM table_data
                WHERE col24 is null or col24 LIKE '' or col24 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) has_numbers_count
                FROM table_data
                WHERE regexp_like(col24, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) has_letters_count
                FROM table_data
                WHERE regexp_like(col24, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) has_decimals_count
                FROM table_data
                WHERE regexp_like(col24, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col24, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col24, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col24) date_count
                FROM table_data
                WHERE regexp_like(col24, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col24
                    , CASE 
                    WHEN regexp_like(col24, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col24, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col24, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col24, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col24, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col24, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col24, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col24, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col24, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col24, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col24, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col24 as int))) numeric_max
            , min(try(cast(col24 as int))) numeric_min
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col24 as decimal(18,2)))) decimal_max
            , min(try(cast(col24 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col24,'/',3),'-',split_part(col24,'/',1),'-',split_part(col24,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col24,'/',3),'-',split_part(col24,'/',1),'-',split_part(col24,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col24,'/',1),'-',split_part(col24,'/',2),'-',split_part(col24,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col24,'/',1),'-',split_part(col24,'/',2),'-',split_part(col24,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col24,'-',3),'-',split_part(col24,'-',1),'-',split_part(col24,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col24,'-',3),'-',split_part(col24,'-',1),'-',split_part(col24,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col24
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col24
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
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
                max(try(cast(col24 as int))) max_int
                , min(try(cast(col24 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col24, 1, 4),'-',substr(col24, 5, 2),'-', substr(col24, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col24, 1, 4),'-',substr(col24, 5, 2),'-', substr(col24, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col24 is not null and col24 <> '' and col24 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col24 as date))) max_date6
            , min(try(cast(col24 as date))) min_date6
            FROM table_data
            WHERE col24 is not null and col24 <> '' and col24 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col24'
)
, col25_str (
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
            , count(col25) total_nonnull_count
            , count(distinct col25) distinct_count
            , min(length(col25)) min_charlength
            , max(length(col25)) max_charlength
            , min(col25) alphabet_min
            , max(col25) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col25) null_count
                FROM table_data
                WHERE col25 is null or col25 LIKE '' or col25 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) has_numbers_count
                FROM table_data
                WHERE regexp_like(col25, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) has_letters_count
                FROM table_data
                WHERE regexp_like(col25, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) has_decimals_count
                FROM table_data
                WHERE regexp_like(col25, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col25, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col25, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col25) date_count
                FROM table_data
                WHERE regexp_like(col25, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col25
                    , CASE 
                    WHEN regexp_like(col25, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col25, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col25, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col25, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col25, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col25, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col25, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col25, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col25, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col25, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col25, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col25 as int))) numeric_max
            , min(try(cast(col25 as int))) numeric_min
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col25 as decimal(18,2)))) decimal_max
            , min(try(cast(col25 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col25,'/',3),'-',split_part(col25,'/',1),'-',split_part(col25,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col25,'/',3),'-',split_part(col25,'/',1),'-',split_part(col25,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col25,'/',1),'-',split_part(col25,'/',2),'-',split_part(col25,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col25,'/',1),'-',split_part(col25,'/',2),'-',split_part(col25,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col25,'-',3),'-',split_part(col25,'-',1),'-',split_part(col25,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col25,'-',3),'-',split_part(col25,'-',1),'-',split_part(col25,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col25
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col25
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
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
                max(try(cast(col25 as int))) max_int
                , min(try(cast(col25 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col25, 1, 4),'-',substr(col25, 5, 2),'-', substr(col25, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col25, 1, 4),'-',substr(col25, 5, 2),'-', substr(col25, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col25 is not null and col25 <> '' and col25 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col25 as date))) max_date6
            , min(try(cast(col25 as date))) min_date6
            FROM table_data
            WHERE col25 is not null and col25 <> '' and col25 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col25'
)
, col26_str (
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
            , count(col26) total_nonnull_count
            , count(distinct col26) distinct_count
            , min(length(col26)) min_charlength
            , max(length(col26)) max_charlength
            , min(col26) alphabet_min
            , max(col26) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col26) null_count
                FROM table_data
                WHERE col26 is null or col26 LIKE '' or col26 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) has_numbers_count
                FROM table_data
                WHERE regexp_like(col26, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) has_letters_count
                FROM table_data
                WHERE regexp_like(col26, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) has_decimals_count
                FROM table_data
                WHERE regexp_like(col26, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col26, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col26, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col26) date_count
                FROM table_data
                WHERE regexp_like(col26, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col26
                    , CASE 
                    WHEN regexp_like(col26, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col26, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col26, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col26, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col26, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col26, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col26, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col26, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col26, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col26, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col26, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col26 as int))) numeric_max
            , min(try(cast(col26 as int))) numeric_min
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col26 as decimal(18,2)))) decimal_max
            , min(try(cast(col26 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col26,'/',3),'-',split_part(col26,'/',1),'-',split_part(col26,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col26,'/',3),'-',split_part(col26,'/',1),'-',split_part(col26,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col26,'/',1),'-',split_part(col26,'/',2),'-',split_part(col26,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col26,'/',1),'-',split_part(col26,'/',2),'-',split_part(col26,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col26,'-',3),'-',split_part(col26,'-',1),'-',split_part(col26,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col26,'-',3),'-',split_part(col26,'-',1),'-',split_part(col26,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col26
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col26
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
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
                max(try(cast(col26 as int))) max_int
                , min(try(cast(col26 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col26, 1, 4),'-',substr(col26, 5, 2),'-', substr(col26, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col26, 1, 4),'-',substr(col26, 5, 2),'-', substr(col26, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col26 is not null and col26 <> '' and col26 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col26 as date))) max_date6
            , min(try(cast(col26 as date))) min_date6
            FROM table_data
            WHERE col26 is not null and col26 <> '' and col26 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col26'
)
, col27_str (
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
            , count(col27) total_nonnull_count
            , count(distinct col27) distinct_count
            , min(length(col27)) min_charlength
            , max(length(col27)) max_charlength
            , min(col27) alphabet_min
            , max(col27) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col27) null_count
                FROM table_data
                WHERE col27 is null or col27 LIKE '' or col27 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) has_numbers_count
                FROM table_data
                WHERE regexp_like(col27, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) has_letters_count
                FROM table_data
                WHERE regexp_like(col27, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) has_decimals_count
                FROM table_data
                WHERE regexp_like(col27, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col27, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col27, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col27) date_count
                FROM table_data
                WHERE regexp_like(col27, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col27
                    , CASE 
                    WHEN regexp_like(col27, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col27, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col27, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col27, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col27, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col27, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col27, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col27, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col27, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col27, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col27, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col27 as int))) numeric_max
            , min(try(cast(col27 as int))) numeric_min
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col27 as decimal(18,2)))) decimal_max
            , min(try(cast(col27 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col27,'/',3),'-',split_part(col27,'/',1),'-',split_part(col27,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col27,'/',3),'-',split_part(col27,'/',1),'-',split_part(col27,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col27,'/',1),'-',split_part(col27,'/',2),'-',split_part(col27,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col27,'/',1),'-',split_part(col27,'/',2),'-',split_part(col27,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col27,'-',3),'-',split_part(col27,'-',1),'-',split_part(col27,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col27,'-',3),'-',split_part(col27,'-',1),'-',split_part(col27,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col27
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col27
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
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
                max(try(cast(col27 as int))) max_int
                , min(try(cast(col27 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col27, 1, 4),'-',substr(col27, 5, 2),'-', substr(col27, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col27, 1, 4),'-',substr(col27, 5, 2),'-', substr(col27, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col27 is not null and col27 <> '' and col27 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col27 as date))) max_date6
            , min(try(cast(col27 as date))) min_date6
            FROM table_data
            WHERE col27 is not null and col27 <> '' and col27 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col27'
)
, col28_str (
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
            , count(col28) total_nonnull_count
            , count(distinct col28) distinct_count
            , min(length(col28)) min_charlength
            , max(length(col28)) max_charlength
            , min(col28) alphabet_min
            , max(col28) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col28) null_count
                FROM table_data
                WHERE col28 is null or col28 LIKE '' or col28 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) has_numbers_count
                FROM table_data
                WHERE regexp_like(col28, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) has_letters_count
                FROM table_data
                WHERE regexp_like(col28, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) has_decimals_count
                FROM table_data
                WHERE regexp_like(col28, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col28, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col28, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col28) date_count
                FROM table_data
                WHERE regexp_like(col28, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col28
                    , CASE 
                    WHEN regexp_like(col28, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col28, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col28, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col28, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col28, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col28, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col28, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col28, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col28, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col28, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col28, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col28 as int))) numeric_max
            , min(try(cast(col28 as int))) numeric_min
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col28 as decimal(18,2)))) decimal_max
            , min(try(cast(col28 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col28,'/',3),'-',split_part(col28,'/',1),'-',split_part(col28,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col28,'/',3),'-',split_part(col28,'/',1),'-',split_part(col28,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col28,'/',1),'-',split_part(col28,'/',2),'-',split_part(col28,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col28,'/',1),'-',split_part(col28,'/',2),'-',split_part(col28,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col28,'-',3),'-',split_part(col28,'-',1),'-',split_part(col28,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col28,'-',3),'-',split_part(col28,'-',1),'-',split_part(col28,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col28
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col28
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
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
                max(try(cast(col28 as int))) max_int
                , min(try(cast(col28 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col28, 1, 4),'-',substr(col28, 5, 2),'-', substr(col28, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col28, 1, 4),'-',substr(col28, 5, 2),'-', substr(col28, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col28 is not null and col28 <> '' and col28 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col28 as date))) max_date6
            , min(try(cast(col28 as date))) min_date6
            FROM table_data
            WHERE col28 is not null and col28 <> '' and col28 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col28'
)
, col29_str (
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
            , count(col29) total_nonnull_count
            , count(distinct col29) distinct_count
            , min(length(col29)) min_charlength
            , max(length(col29)) max_charlength
            , min(col29) alphabet_min
            , max(col29) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col29) null_count
                FROM table_data
                WHERE col29 is null or col29 LIKE '' or col29 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) has_numbers_count
                FROM table_data
                WHERE regexp_like(col29, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) has_letters_count
                FROM table_data
                WHERE regexp_like(col29, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) has_decimals_count
                FROM table_data
                WHERE regexp_like(col29, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col29, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col29, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col29) date_count
                FROM table_data
                WHERE regexp_like(col29, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col29
                    , CASE 
                    WHEN regexp_like(col29, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col29, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col29, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col29, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col29, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col29, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col29, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col29, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col29, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col29, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col29, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col29 as int))) numeric_max
            , min(try(cast(col29 as int))) numeric_min
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col29 as decimal(18,2)))) decimal_max
            , min(try(cast(col29 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col29,'/',3),'-',split_part(col29,'/',1),'-',split_part(col29,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col29,'/',3),'-',split_part(col29,'/',1),'-',split_part(col29,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col29,'/',1),'-',split_part(col29,'/',2),'-',split_part(col29,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col29,'/',1),'-',split_part(col29,'/',2),'-',split_part(col29,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col29,'-',3),'-',split_part(col29,'-',1),'-',split_part(col29,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col29,'-',3),'-',split_part(col29,'-',1),'-',split_part(col29,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col29
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col29
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
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
                max(try(cast(col29 as int))) max_int
                , min(try(cast(col29 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col29, 1, 4),'-',substr(col29, 5, 2),'-', substr(col29, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col29, 1, 4),'-',substr(col29, 5, 2),'-', substr(col29, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col29 is not null and col29 <> '' and col29 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col29 as date))) max_date6
            , min(try(cast(col29 as date))) min_date6
            FROM table_data
            WHERE col29 is not null and col29 <> '' and col29 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col29'
)
, col30_str (
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
            , count(col30) total_nonnull_count
            , count(distinct col30) distinct_count
            , min(length(col30)) min_charlength
            , max(length(col30)) max_charlength
            , min(col30) alphabet_min
            , max(col30) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col30) null_count
                FROM table_data
                WHERE col30 is null or col30 LIKE '' or col30 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) has_numbers_count
                FROM table_data
                WHERE regexp_like(col30, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) has_letters_count
                FROM table_data
                WHERE regexp_like(col30, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) has_decimals_count
                FROM table_data
                WHERE regexp_like(col30, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col30, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col30, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col30) date_count
                FROM table_data
                WHERE regexp_like(col30, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col30
                    , CASE 
                    WHEN regexp_like(col30, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col30, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col30, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col30, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col30, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col30, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col30, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col30, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col30, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col30, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col30, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col30 as int))) numeric_max
            , min(try(cast(col30 as int))) numeric_min
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col30 as decimal(18,2)))) decimal_max
            , min(try(cast(col30 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col30,'/',3),'-',split_part(col30,'/',1),'-',split_part(col30,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col30,'/',3),'-',split_part(col30,'/',1),'-',split_part(col30,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col30,'/',1),'-',split_part(col30,'/',2),'-',split_part(col30,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col30,'/',1),'-',split_part(col30,'/',2),'-',split_part(col30,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col30,'-',3),'-',split_part(col30,'-',1),'-',split_part(col30,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col30,'-',3),'-',split_part(col30,'-',1),'-',split_part(col30,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col30
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col30
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
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
                max(try(cast(col30 as int))) max_int
                , min(try(cast(col30 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col30, 1, 4),'-',substr(col30, 5, 2),'-', substr(col30, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col30, 1, 4),'-',substr(col30, 5, 2),'-', substr(col30, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col30 is not null and col30 <> '' and col30 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col30 as date))) max_date6
            , min(try(cast(col30 as date))) min_date6
            FROM table_data
            WHERE col30 is not null and col30 <> '' and col30 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col30'
)
, combined as (
SELECT * FROM col21_str
UNION SELECT * FROM col22_str
UNION SELECT * FROM col23_str
UNION SELECT * FROM col24_str
UNION SELECT * FROM col25_str
UNION SELECT * FROM col26_str
UNION SELECT * FROM col27_str
UNION SELECT * FROM col28_str
UNION SELECT * FROM col29_str
UNION SELECT * FROM col30_str
)
SELECT * FROM combined ORDER BY ordinal_position;