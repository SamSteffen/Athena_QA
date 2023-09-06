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
, col1_str (
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
            , count(col1) total_nonnull_count
            , count(distinct col1) distinct_count
            , min(length(col1)) min_charlength
            , max(length(col1)) max_charlength
            , min(col1) alphabet_min
            , max(col1) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col1) null_count
                FROM table_data
                WHERE col1 is null or col1 LIKE '' or col1 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_numbers_count
                FROM table_data
                WHERE regexp_like(col1, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_letters_count
                FROM table_data
                WHERE regexp_like(col1, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_decimals_count
                FROM table_data
                WHERE regexp_like(col1, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col1, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col1, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col1) date_count
                FROM table_data
                WHERE regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col1
                    , CASE 
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col1, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col1, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col1, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col1, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col1, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col1, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col1, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col1, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col1 as int))) numeric_max
            , min(try(cast(col1 as int))) numeric_min
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col1 as decimal(18,2)))) decimal_max
            , min(try(cast(col1 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'/',3),'-',split_part(col1,'/',1),'-',split_part(col1,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'/',1),'-',split_part(col1,'/',2),'-',split_part(col1,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col1,'-',3),'-',split_part(col1,'-',1),'-',split_part(col1,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col1
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col1
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
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
                max(try(cast(col1 as int))) max_int
                , min(try(cast(col1 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col1, 1, 4),'-',substr(col1, 5, 2),'-', substr(col1, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col1 is not null and col1 <> '' and col1 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col1 as date))) max_date6
            , min(try(cast(col1 as date))) min_date6
            FROM table_data
            WHERE col1 is not null and col1 <> '' and col1 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col1'
)
, col2_str (
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
            , count(col2) total_nonnull_count
            , count(distinct col2) distinct_count
            , min(length(col2)) min_charlength
            , max(length(col2)) max_charlength
            , min(col2) alphabet_min
            , max(col2) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col2) null_count
                FROM table_data
                WHERE col2 is null or col2 LIKE '' or col2 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) has_numbers_count
                FROM table_data
                WHERE regexp_like(col2, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) has_letters_count
                FROM table_data
                WHERE regexp_like(col2, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) has_decimals_count
                FROM table_data
                WHERE regexp_like(col2, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col2, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col2, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col2) date_count
                FROM table_data
                WHERE regexp_like(col2, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col2
                    , CASE 
                    WHEN regexp_like(col2, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col2, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col2, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col2, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col2, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col2, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col2, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col2, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col2, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col2, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col2, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col2 as int))) numeric_max
            , min(try(cast(col2 as int))) numeric_min
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col2 as decimal(18,2)))) decimal_max
            , min(try(cast(col2 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col2,'/',3),'-',split_part(col2,'/',1),'-',split_part(col2,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col2,'/',3),'-',split_part(col2,'/',1),'-',split_part(col2,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col2,'/',1),'-',split_part(col2,'/',2),'-',split_part(col2,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col2,'/',1),'-',split_part(col2,'/',2),'-',split_part(col2,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col2,'-',3),'-',split_part(col2,'-',1),'-',split_part(col2,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col2,'-',3),'-',split_part(col2,'-',1),'-',split_part(col2,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col2
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col2
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
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
                max(try(cast(col2 as int))) max_int
                , min(try(cast(col2 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col2, 1, 4),'-',substr(col2, 5, 2),'-', substr(col2, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col2, 1, 4),'-',substr(col2, 5, 2),'-', substr(col2, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col2 is not null and col2 <> '' and col2 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col2 as date))) max_date6
            , min(try(cast(col2 as date))) min_date6
            FROM table_data
            WHERE col2 is not null and col2 <> '' and col2 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col2'
)
, col3_str (
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
            , count(col3) total_nonnull_count
            , count(distinct col3) distinct_count
            , min(length(col3)) min_charlength
            , max(length(col3)) max_charlength
            , min(col3) alphabet_min
            , max(col3) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col3) null_count
                FROM table_data
                WHERE col3 is null or col3 LIKE '' or col3 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) has_numbers_count
                FROM table_data
                WHERE regexp_like(col3, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) has_letters_count
                FROM table_data
                WHERE regexp_like(col3, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) has_decimals_count
                FROM table_data
                WHERE regexp_like(col3, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col3, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col3, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col3) date_count
                FROM table_data
                WHERE regexp_like(col3, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col3
                    , CASE 
                    WHEN regexp_like(col3, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col3, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col3, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col3, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col3, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col3, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col3, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col3, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col3, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col3, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col3, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col3 as int))) numeric_max
            , min(try(cast(col3 as int))) numeric_min
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col3 as decimal(18,2)))) decimal_max
            , min(try(cast(col3 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col3,'/',3),'-',split_part(col3,'/',1),'-',split_part(col3,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col3,'/',3),'-',split_part(col3,'/',1),'-',split_part(col3,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col3,'/',1),'-',split_part(col3,'/',2),'-',split_part(col3,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col3,'/',1),'-',split_part(col3,'/',2),'-',split_part(col3,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col3,'-',3),'-',split_part(col3,'-',1),'-',split_part(col3,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col3,'-',3),'-',split_part(col3,'-',1),'-',split_part(col3,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col3
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col3
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
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
                max(try(cast(col3 as int))) max_int
                , min(try(cast(col3 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col3, 1, 4),'-',substr(col3, 5, 2),'-', substr(col3, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col3, 1, 4),'-',substr(col3, 5, 2),'-', substr(col3, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col3 is not null and col3 <> '' and col3 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col3 as date))) max_date6
            , min(try(cast(col3 as date))) min_date6
            FROM table_data
            WHERE col3 is not null and col3 <> '' and col3 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col3'
)
, col4_str (
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
            , count(col4) total_nonnull_count
            , count(distinct col4) distinct_count
            , min(length(col4)) min_charlength
            , max(length(col4)) max_charlength
            , min(col4) alphabet_min
            , max(col4) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col4) null_count
                FROM table_data
                WHERE col4 is null or col4 LIKE '' or col4 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) has_numbers_count
                FROM table_data
                WHERE regexp_like(col4, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) has_letters_count
                FROM table_data
                WHERE regexp_like(col4, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) has_decimals_count
                FROM table_data
                WHERE regexp_like(col4, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col4, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col4, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col4) date_count
                FROM table_data
                WHERE regexp_like(col4, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col4
                    , CASE 
                    WHEN regexp_like(col4, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col4, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col4, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col4, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col4, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col4, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col4, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col4, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col4, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col4, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col4, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col4 as int))) numeric_max
            , min(try(cast(col4 as int))) numeric_min
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col4 as decimal(18,2)))) decimal_max
            , min(try(cast(col4 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col4,'/',3),'-',split_part(col4,'/',1),'-',split_part(col4,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col4,'/',3),'-',split_part(col4,'/',1),'-',split_part(col4,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col4,'/',1),'-',split_part(col4,'/',2),'-',split_part(col4,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col4,'/',1),'-',split_part(col4,'/',2),'-',split_part(col4,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col4,'-',3),'-',split_part(col4,'-',1),'-',split_part(col4,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col4,'-',3),'-',split_part(col4,'-',1),'-',split_part(col4,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col4
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col4
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
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
                max(try(cast(col4 as int))) max_int
                , min(try(cast(col4 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col4, 1, 4),'-',substr(col4, 5, 2),'-', substr(col4, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col4, 1, 4),'-',substr(col4, 5, 2),'-', substr(col4, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col4 is not null and col4 <> '' and col4 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col4 as date))) max_date6
            , min(try(cast(col4 as date))) min_date6
            FROM table_data
            WHERE col4 is not null and col4 <> '' and col4 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col4'
)
, col5_str (
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
            , count(col5) total_nonnull_count
            , count(distinct col5) distinct_count
            , min(length(col5)) min_charlength
            , max(length(col5)) max_charlength
            , min(col5) alphabet_min
            , max(col5) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col5) null_count
                FROM table_data
                WHERE col5 is null or col5 LIKE '' or col5 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) has_numbers_count
                FROM table_data
                WHERE regexp_like(col5, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) has_letters_count
                FROM table_data
                WHERE regexp_like(col5, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) has_decimals_count
                FROM table_data
                WHERE regexp_like(col5, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col5, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col5, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col5) date_count
                FROM table_data
                WHERE regexp_like(col5, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col5
                    , CASE 
                    WHEN regexp_like(col5, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col5, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col5, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col5, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col5, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col5, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col5, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col5, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col5, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col5, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col5, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col5 as int))) numeric_max
            , min(try(cast(col5 as int))) numeric_min
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col5 as decimal(18,2)))) decimal_max
            , min(try(cast(col5 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col5,'/',3),'-',split_part(col5,'/',1),'-',split_part(col5,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col5,'/',3),'-',split_part(col5,'/',1),'-',split_part(col5,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col5,'/',1),'-',split_part(col5,'/',2),'-',split_part(col5,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col5,'/',1),'-',split_part(col5,'/',2),'-',split_part(col5,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col5,'-',3),'-',split_part(col5,'-',1),'-',split_part(col5,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col5,'-',3),'-',split_part(col5,'-',1),'-',split_part(col5,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col5
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col5
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
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
                max(try(cast(col5 as int))) max_int
                , min(try(cast(col5 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col5, 1, 4),'-',substr(col5, 5, 2),'-', substr(col5, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col5, 1, 4),'-',substr(col5, 5, 2),'-', substr(col5, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col5 is not null and col5 <> '' and col5 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col5 as date))) max_date6
            , min(try(cast(col5 as date))) min_date6
            FROM table_data
            WHERE col5 is not null and col5 <> '' and col5 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col5'
)
, col6_str (
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
            , count(col6) total_nonnull_count
            , count(distinct col6) distinct_count
            , min(length(col6)) min_charlength
            , max(length(col6)) max_charlength
            , min(col6) alphabet_min
            , max(col6) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col6) null_count
                FROM table_data
                WHERE col6 is null or col6 LIKE '' or col6 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) has_numbers_count
                FROM table_data
                WHERE regexp_like(col6, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) has_letters_count
                FROM table_data
                WHERE regexp_like(col6, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) has_decimals_count
                FROM table_data
                WHERE regexp_like(col6, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col6, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col6, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col6) date_count
                FROM table_data
                WHERE regexp_like(col6, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col6
                    , CASE 
                    WHEN regexp_like(col6, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col6, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col6, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col6, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col6, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col6, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col6, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col6, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col6, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col6, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col6, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col6 as int))) numeric_max
            , min(try(cast(col6 as int))) numeric_min
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col6 as decimal(18,2)))) decimal_max
            , min(try(cast(col6 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col6,'/',3),'-',split_part(col6,'/',1),'-',split_part(col6,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col6,'/',3),'-',split_part(col6,'/',1),'-',split_part(col6,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col6,'/',1),'-',split_part(col6,'/',2),'-',split_part(col6,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col6,'/',1),'-',split_part(col6,'/',2),'-',split_part(col6,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col6,'-',3),'-',split_part(col6,'-',1),'-',split_part(col6,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col6,'-',3),'-',split_part(col6,'-',1),'-',split_part(col6,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col6
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col6
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
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
                max(try(cast(col6 as int))) max_int
                , min(try(cast(col6 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col6, 1, 4),'-',substr(col6, 5, 2),'-', substr(col6, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col6, 1, 4),'-',substr(col6, 5, 2),'-', substr(col6, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col6 is not null and col6 <> '' and col6 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col6 as date))) max_date6
            , min(try(cast(col6 as date))) min_date6
            FROM table_data
            WHERE col6 is not null and col6 <> '' and col6 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col6'
)
, col7_str (
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
            , count(col7) total_nonnull_count
            , count(distinct col7) distinct_count
            , min(length(col7)) min_charlength
            , max(length(col7)) max_charlength
            , min(col7) alphabet_min
            , max(col7) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col7) null_count
                FROM table_data
                WHERE col7 is null or col7 LIKE '' or col7 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) has_numbers_count
                FROM table_data
                WHERE regexp_like(col7, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) has_letters_count
                FROM table_data
                WHERE regexp_like(col7, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) has_decimals_count
                FROM table_data
                WHERE regexp_like(col7, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col7, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col7, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col7) date_count
                FROM table_data
                WHERE regexp_like(col7, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col7
                    , CASE 
                    WHEN regexp_like(col7, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col7, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col7, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col7, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col7, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col7, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col7, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col7, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col7, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col7, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col7, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col7 as int))) numeric_max
            , min(try(cast(col7 as int))) numeric_min
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col7 as decimal(18,2)))) decimal_max
            , min(try(cast(col7 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col7,'/',3),'-',split_part(col7,'/',1),'-',split_part(col7,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col7,'/',3),'-',split_part(col7,'/',1),'-',split_part(col7,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col7,'/',1),'-',split_part(col7,'/',2),'-',split_part(col7,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col7,'/',1),'-',split_part(col7,'/',2),'-',split_part(col7,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col7,'-',3),'-',split_part(col7,'-',1),'-',split_part(col7,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col7,'-',3),'-',split_part(col7,'-',1),'-',split_part(col7,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col7
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col7
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
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
                max(try(cast(col7 as int))) max_int
                , min(try(cast(col7 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col7, 1, 4),'-',substr(col7, 5, 2),'-', substr(col7, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col7, 1, 4),'-',substr(col7, 5, 2),'-', substr(col7, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col7 is not null and col7 <> '' and col7 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col7 as date))) max_date6
            , min(try(cast(col7 as date))) min_date6
            FROM table_data
            WHERE col7 is not null and col7 <> '' and col7 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col7'
)
, col8_str (
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
            , count(col8) total_nonnull_count
            , count(distinct col8) distinct_count
            , min(length(col8)) min_charlength
            , max(length(col8)) max_charlength
            , min(col8) alphabet_min
            , max(col8) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col8) null_count
                FROM table_data
                WHERE col8 is null or col8 LIKE '' or col8 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) has_numbers_count
                FROM table_data
                WHERE regexp_like(col8, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) has_letters_count
                FROM table_data
                WHERE regexp_like(col8, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) has_decimals_count
                FROM table_data
                WHERE regexp_like(col8, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col8, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col8, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col8) date_count
                FROM table_data
                WHERE regexp_like(col8, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col8
                    , CASE 
                    WHEN regexp_like(col8, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col8, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col8, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col8, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col8, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col8, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col8, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col8, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col8, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col8, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col8, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col8 as int))) numeric_max
            , min(try(cast(col8 as int))) numeric_min
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col8 as decimal(18,2)))) decimal_max
            , min(try(cast(col8 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col8,'/',3),'-',split_part(col8,'/',1),'-',split_part(col8,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col8,'/',3),'-',split_part(col8,'/',1),'-',split_part(col8,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col8,'/',1),'-',split_part(col8,'/',2),'-',split_part(col8,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col8,'/',1),'-',split_part(col8,'/',2),'-',split_part(col8,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col8,'-',3),'-',split_part(col8,'-',1),'-',split_part(col8,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col8,'-',3),'-',split_part(col8,'-',1),'-',split_part(col8,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col8
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col8
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
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
                max(try(cast(col8 as int))) max_int
                , min(try(cast(col8 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col8, 1, 4),'-',substr(col8, 5, 2),'-', substr(col8, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col8, 1, 4),'-',substr(col8, 5, 2),'-', substr(col8, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col8 is not null and col8 <> '' and col8 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col8 as date))) max_date6
            , min(try(cast(col8 as date))) min_date6
            FROM table_data
            WHERE col8 is not null and col8 <> '' and col8 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col8'
)
, col9_str (
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
            , count(col9) total_nonnull_count
            , count(distinct col9) distinct_count
            , min(length(col9)) min_charlength
            , max(length(col9)) max_charlength
            , min(col9) alphabet_min
            , max(col9) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col9) null_count
                FROM table_data
                WHERE col9 is null or col9 LIKE '' or col9 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) has_numbers_count
                FROM table_data
                WHERE regexp_like(col9, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) has_letters_count
                FROM table_data
                WHERE regexp_like(col9, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) has_decimals_count
                FROM table_data
                WHERE regexp_like(col9, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col9, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col9, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col9) date_count
                FROM table_data
                WHERE regexp_like(col9, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col9
                    , CASE 
                    WHEN regexp_like(col9, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col9, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col9, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col9, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col9, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col9, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col9, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col9, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col9, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col9, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col9, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col9 as int))) numeric_max
            , min(try(cast(col9 as int))) numeric_min
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col9 as decimal(18,2)))) decimal_max
            , min(try(cast(col9 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col9,'/',3),'-',split_part(col9,'/',1),'-',split_part(col9,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col9,'/',3),'-',split_part(col9,'/',1),'-',split_part(col9,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col9,'/',1),'-',split_part(col9,'/',2),'-',split_part(col9,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col9,'/',1),'-',split_part(col9,'/',2),'-',split_part(col9,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col9,'-',3),'-',split_part(col9,'-',1),'-',split_part(col9,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col9,'-',3),'-',split_part(col9,'-',1),'-',split_part(col9,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col9
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col9
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
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
                max(try(cast(col9 as int))) max_int
                , min(try(cast(col9 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col9, 1, 4),'-',substr(col9, 5, 2),'-', substr(col9, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col9, 1, 4),'-',substr(col9, 5, 2),'-', substr(col9, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col9 is not null and col9 <> '' and col9 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col9 as date))) max_date6
            , min(try(cast(col9 as date))) min_date6
            FROM table_data
            WHERE col9 is not null and col9 <> '' and col9 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col9'
)
, col10_str (
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
            , count(col10) total_nonnull_count
            , count(distinct col10) distinct_count
            , min(length(col10)) min_charlength
            , max(length(col10)) max_charlength
            , min(col10) alphabet_min
            , max(col10) alphabet_max
            , b.has_numbers_count
            , c.has_letters_count
            , d.has_decimals_count
            , e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count
            , g.date_count
            , h.dateformat
            FROM table_data
            FULL OUTER JOIN (
                SELECT count(col10) null_count
                FROM table_data
                WHERE col10 is null or col10 LIKE '' or col10 LIKE ' '
            ) a
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) has_numbers_count
                FROM table_data
                WHERE regexp_like(col10, '\d')=True
            ) b
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) has_letters_count
                FROM table_data
                WHERE regexp_like(col10, '[A-Za-z]')=True
            ) c
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) has_decimals_count
                FROM table_data
                WHERE regexp_like(col10, '\.')=True
            ) d
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) has_non_alphanumeric_characters_count
                FROM table_data
                WHERE regexp_like(col10, '\!|\@|\#|\$|\%|\^|\&|\*|\+|\-|\/|\\|\<|\>|\,|\.|\?|\||\''|\"|\_|\|\')=True
            ) e
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) decimal_datatype_count
                FROM table_data
                WHERE regexp_like(col10, '^\d+\.\d+')=True
            ) f
            ON 1=1
            FULL OUTER JOIN (
                SELECT count(col10) date_count
                FROM table_data
                WHERE regexp_like(col10, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s
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
                    col10
                    , CASE 
                    WHEN regexp_like(col10, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}\s')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm ZON'
                    WHEN regexp_like(col10, '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\:\d{2}\.\d{3}$')
                        THEN 'yyyy-mm-dd hh:mm:ss.mmm'
                    WHEN regexp_like(col10, '\d{4}\-\d{2}\-\d{2}$')
                        THEN 'yyyy-mm-dd'
                    WHEN regexp_like(col10, '\d{4}\-\d{1,2}\-\d{1,2}$')
                        THEN 'yyyy-m-d'
                    WHEN regexp_like(col10, '\d{2}\-\d{2}\-\d{4}')
                        THEN 'mm-dd-yyyy'
                    WHEN regexp_like(col10, '\d{1,2}\-\d{1,2}\-\d{4}')
                        THEN 'm-d-yyyy'
                    WHEN regexp_like(col10, '\d{4}\/\d{2}\/\d{2}$')
                        THEN 'yyyy/mm/dd'
                    WHEN regexp_like(col10, '\d{4}\/\d{1,2}\/\d{1,2}$')
                        THEN 'yyyy/m/d'
                    WHEN regexp_like(col10, '\d{2}\/\d{2}\/\d{4}')
                        THEN 'mm/dd/yyyy'
                    WHEN regexp_like(col10, '\d{1,2}\/\d{1,2}\/\d{4}$')
                        THEN 'm/d/yyyy'
                    WHEN regexp_like(col10, '^\d{8}$')
                        THEN 'yyyymmdd'
                    ELSE 'N/A' END as date_format
                    FROM table_data
                )
                GROUP BY dateformat
            ) h
            ON 1=1
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
            GROUP BY 
            a.null_count, b.has_numbers_count, c.has_letters_count
            , d.has_decimals_count, e.has_non_alphanumeric_characters_count
            , f.decimal_datatype_count, g.date_count, h.dateformat
        ) a
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col10 as int))) numeric_max
            , min(try(cast(col10 as int))) numeric_min
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) b
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(try(cast(col10 as decimal(18,2)))) decimal_max
            , min(try(cast(col10 as decima(18,2)))) decimal_min
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) c
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm/dd/yyyy' OR 'm/d/yyyy' use:
                            split_part(col10,'/',3),'-',split_part(col10,'/',1),'-',split_part(col10,'/',2)
                            ) 
                        as date)
                    )
                ) max_date1
            , min(
                try(
                    cast(
                        concat(
                            split_part(col10,'/',3),'-',split_part(col10,'/',1),'-',split_part(col10,'/',2)
                            )
                        as date)
                    )
                ) min_date1
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) d
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'yyyy/mm/dd' OR 'yyyy/m/d' use:
                            split_part(col10,'/',1),'-',split_part(col10,'/',2),'-',split_part(col10,'/',3)
                            ) 
                        as date)
                    )
                ) max_date2
            , min(
                try(
                    cast(
                        concat(
                            split_part(col10,'/',1),'-',split_part(col10,'/',2),'-',split_part(col10,'/',3)
                            )
                        as date)
                    )
                ) min_date2
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) e
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        concat(
                            --when data is formatted as 'mm-dd-yyyy' OR 'm-d-yyyy' use:
                            split_part(col10,'-',3),'-',split_part(col10,'-',1),'-',split_part(col10,'-',2)
                            ) 
                        as date)
                    )
                ) max_date3
            , min(
                try(
                    cast(
                        concat(
                            split_part(col10,'-',3),'-',split_part(col10,'-',1),'-',split_part(col10,'-',2)
                            )
                        as date)
                    )
                ) min_date3
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) f
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            max(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col10
                        as date)
                    )
                ) max_date4
            , min(
                try(
                    cast(
                        --when data is formatted as 'yyyy-mm-dd' OR 'yyyy-m-d' use:
                        col10
                        as date)
                    )
                ) min_date4
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
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
                max(try(cast(col10 as int))) max_int
                , min(try(cast(col10 as int))) min_int
                , max(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col10, 1, 4),'-',substr(col10, 5, 2),'-', substr(col10, 7, 2)
                                )
                            as date)
                        )
                    ) max_date
                , min(
                    try(
                        cast(
                            concat(
                                --when data is formatted as 'yyyymmdd' use:
                                substr(col10, 1, 4),'-',substr(col10, 5, 2),'-', substr(col10, 7, 2)
                                )
                            as date)
                        )
                    ) min_date
                FROM table_data
                WHERE col10 is not null and col10 <> '' and col10 <> ' '
            ) A
        ) h
        ON 1=1
        FULL OUTER JOIN (
            SELECT
            --when data is formatted as 'yyyy-mm-dd 00:00:00.000 ZON' OR 'yyyy-mm-dd 00:00:00.000' use:
            max(try(cast(col10 as date))) max_date6
            , min(try(cast(col10 as date))) min_date6
            FROM table_data
            WHERE col10 is not null and col10 <> '' and col10 <> ' '
        ) i
        ON 1=1
    ) a
    JOIN metadata2 md 
    on 1=1
    WHERE md.temp_column_name = 'col10'
)
, combined as (
SELECT * from col1_str
UNION SELECT * from col2_str
UNION SELECT * FROM col3_str
UNION SELECT * FROM col4_str
UNION SELECT * FROM col5_str
UNION SELECT * FROM col6_str
UNION SELECT * FROM col7_str
UNION SELECT * FROM col8_str
UNION SELECT * FROM col9_str
UNION SELECT * FROM col10_str
)
SELECT * FROM combined ORDER BY ordinal_position;
