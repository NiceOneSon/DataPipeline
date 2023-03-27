class NiceOneSon_SQL_Queries:

    create_temp_table = """ create table {0}.{1}.tmp_{2} as 
                            select * 
                              from {0}.{1}.{2}
                        """
    truncate_origin_table = """
                            truncate table {0}.{1}.{2}
                            """
    merge_tables = """
                    insert into stock_dataset.stock_detail(itmsNm, mrktCtg, clpr, vs, fltRt
                            ,mkp ,hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, update, modified, basDt)
                    select itmsNm, mrktCtg, clpr, vs, fltRt
                        ,mkp ,hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, update, modified, basDt
                    from
                        (SELECT *, row_number() over(partition by basDt, itmsNm, mrktCtg order by update desc) as rnum
                        FROM stock_dataset.tmp_stock_detail
                        )
                    where rnum = 1
                    """
    drop_temp_table = """
                     drop table {0}.{1}.tmp_{2}
                     """
    