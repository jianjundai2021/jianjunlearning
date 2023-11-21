-- this is to calculate a single hash value for the entire table, row level hash has been calculated in the previous step
-- to do this single hash calculation, we need to loop through the table, concatenate the previous hash value
-- with the current one and keep doing this for all records in the table.

-- note for sql server 1 minute for 1.5M records
-- 30 minutes for 60M records 



select * from DM$OUTPUT_HASH.TEST_NATION order by pk_value;



Alter PROCEDURE LoopThroughTable
AS
BEGIN
    -- Declare a cursor
    DECLARE myCursor CURSOR FOR
    SELECT output_attributes_hash_varchar FROM DM$OUTPUT_HASH.TEST_NATION order by pk_value

    -- Variables to hold data
    DECLARE @CurrentColumnData varchar(255)
       DECLARE @CONCATEVALUE varchar(8000)
       -- DECLARE @CONCATEVALUE varbinary(8000)
       DECLARE @IsFirstRecord BIT = 1
       declare @prevdata varchar(8000)

    -- Open the cursor
    OPEN myCursor

    -- Fetch the first row from the cursor
    FETCH NEXT FROM myCursor INTO @CurrentColumnData

    -- Loop through all rows
       set @CONCATEVALUE =  ''
       print @CONCATEVALUE
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Here, you can perform operations with @CurrentColumnData

              -- print @CurrentColumnData

        -- set @CONCATEVALUE = convert(varchar(64), hashbytes('SHA_256', convert(varbinary, @CONCATEVALUE + @CurrentColumnData))) 
              -- set @CONCATEVALUE = convert(varbinary, @CONCATEVALUE + @CurrentColumnData)
              -- set @CONCATEVALUE = hashbytes('SHA_256', convert(varchar, @CONCATEVALUE) + @CurrentColumnData)
              if @IsFirstRecord = 1 
              begin 
                     set @IsFirstRecord = 0 
                     print @CurrentColumnData
                     set @prevdata = @CurrentColumnData 
              end
              else 
              begin
                     print @prevdata + @CurrentColumnData
                     set @prevdata = @CurrentColumnData 
              end 

        -- Fetch the next row from the cursor
        FETCH NEXT FROM myCursor INTO @CurrentColumnData
    END

    -- Close and deallocate the cursor
    CLOSE myCursor
    DEALLOCATE myCursor
END

exec LoopThroughTable


