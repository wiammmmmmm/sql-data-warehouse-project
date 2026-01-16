/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin

declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
 begin try

 set @batch_start_time = GETDATE();
   print '==========================================' 
	print'Loading Silver layer '                                       
	print '=========================================='
     
    print '----------------------------------------------'
	print 'Loading CRM Tables'
	print '----------------------------------------------'

    set @start_time = getdate();
    print 'truncating Table : Silver.crm_cust_info '
    truncate table Silver.crm_cust_info;

    print 'Inserting Dta into : silver.cust_info'
    insert into Silver.crm_cust_info ( 
    cst_id ,
    cst_Key , 
    cst_firstname ,
    cst_lastname ,
    cst_material_status ,
    cst_gndr ,
    cst_create_data 
    )

    select
    cst_id ,
    cst_Key ,
    -- removing unwanted spaces
    trim(cst_firstname) as cst_firstname ,
    trim(cst_lastname) as cst_lastname ,
    -- data normalization & standardization
    case when upper(trim(cst_material_status)) = 'S' then 'Single'
         when upper(trim(cst_material_status)) = 'M' then 'Married'
         -- handling missing data
         else 'n/a'
         end cst_material_status ,
    case when upper(trim(cst_gndr)) = 'F' then 'Female'
         when upper(trim(cst_gndr)) = 'M' then 'Male'
         -- handling missing data
         else 'n/a'
         end cst_gndr,
    cst_create_data 
    -- remove duplicates 
    from (
    select *,
    ROW_NUMBER() over (partition by cst_id 
    order by cst_create_data desc) as flag_last 
    from bronze.crm_cust_info 
    where cst_id is not null ) t where flag_last = 1 -- select the most record customer

    set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'
    

    set @start_time = getdate();
    print 'truncating Table : Silver.crm_prd_info '
    truncate table Silver.crm_prd_info;

    print 'Inserting Dta into : silver.crm_prd_info'
    insert into Silver.crm_prd_info (
        prd_id       ,
        cat_id ,
        prd_key      ,
        prd_nm       ,
        prd_cost     ,
        prd_line     ,
        prd_start_dt ,
        prd_end_dt   
    )
    select  
        prd_id       ,
        -- Derived  create new colums based on transformtions of existing ones
        replace(SUBSTRING(prd_key,1,5), '-','_') as cat_id, -- extract category id
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key, -- extract product key
        prd_nm       ,
        isnull(prd_cost, 0) as prd_cost, -- handeling missing info       
        -- data normalization
        case UPPER(trim(prd_line))
        when  'M' then 'Mountain'
        when  'S' then 'other Sales'
        when  'R' then 'Road'
        when  'T' then 'Touring'
        else 'n/a'
        end as prd_line,
       cast(prd_start_dt as date) as prd_start_dt,
       -- data enrichment
       CAST(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt    -- end date = start of the next record -1

    from Bronze.crm_prd_info

    set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'


    set @start_time = getdate();
    print 'truncating Table : Silver.crm_sales_details '
    truncate table Silver.crm_sales_details;

    print 'Inserting Dta into : silver.crm_sales_details'
    insert into  Silver.crm_sales_details (

     sls_ord_num  ,
        sls_prd_key  ,
        sls_cust_id  ,
        sls_order_dt ,
        sls_ship_dt  ,
        sls_due_dt   ,
        sls_sales    ,
        sls_quantity ,
        sls_price    
    )

    select 
        sls_ord_num  ,
        sls_prd_key  ,
        sls_cust_id  ,
        -- 
        case when sls_order_dt = 0 or len(sls_order_dt) !=8  then NULL -- handling invalid data
             else cast(cast(sls_order_dt as varchar) as date) -- data type  casting
        end as sls_order_dt,
    
        case when sls_ship_dt = 0 or len(sls_ship_dt) !=8  then NULL
             else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
   
       case when sls_due_dt = 0 or len(sls_due_dt) !=8  then NULL
             else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,

      case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) -- handling missing data, invalid by deriving the column from already existing oe  
          then sls_quantity * abs(sls_price) 
          else sls_sales 
    end as sls_sales ,
    
        sls_quantity ,
 
     case when sls_price is null or sls_price <=0
         then sls_sales / nullif(sls_quantity, 0) --  nullif(sls_quantity, 0) => if sls_quantity replace it with 0 
         else sls_price
    end as sls_price 

    from Bronze.crm_sales_details 

    set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'


    print '----------------------------------------------'
	print 'Loading ERP Tables'
	print '----------------------------------------------'

   	set @start_time = getdate();
    print 'truncating Table : Silver.erp_px_cat_g1v2 '
    truncate table Silver.erp_px_cat_g1v2;

    print 'Inserting Dta into : silver.erp_px_cat_g1v2'
    insert into Silver.erp_px_cat_g1v2
    (
    id,
    cat,
    subcat,
    maintenance
    )
    select 
    id,
    cat,
    subcat,
    maintenance 
    from Bronze.erp_px_cat_g1v2
   
   set @end_time = getdate();
    print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'


    set @start_time = getdate();
    print 'truncating Table : Silver.erp_cust_az12 '
    truncate table Silver.erp_cust_az12;

    print 'Inserting Dta into : silver.erp_cust_az12'
    insert into Silver.erp_cust_az12 (
    cid,
    bdate,
    gen
    )
    select    
        case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) -- handling invalid values not needed 'NAS'
             else cid
        end as cid,
    
        case when bdate > GETDATE() then null 
             else bdate
        end as bdate, -- set future birthdates to null

        case when upper(trim(gen)) in ('F', 'Female') then 'Female'
         when upper(trim(gen)) in ('M', 'Male') then 'Male'
         else 'n/a'
    end as gen  -- normalize gender values and handles values
    from Bronze.erp_cust_az12

    set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'


   	set @start_time = getdate();
    print 'truncating Table : Silver.erp_loc_a101 '
    truncate table Silver.erp_loc_a101;

    print 'Inserting Dta into : silver.erp_loc_a101'
    insert into Silver.erp_loc_a101 (
    cid,
    cntry

    )
    select 
    replace(cid,'-','')cid, 

    case when trim(cntry) = 'DE' then 'Germany'
         when trim(cntry)  in ('US', 'USA') then 'United States'
         when trim(cntry) = '' or cntry is null then 'n/a'
         else trim(cntry) 
    end as cntry 

    from Bronze.erp_loc_a101

    set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds'
	print '-------------------------------------------------------'

    set @batch_end_time = GETDATE();
	print 'Loading Silver Layer is completed '
	print 'Totale Load Duration : ' + cast(datediff(second, @batch_start_time , @batch_end_time) as nvarchar) + 'seconds';
	print '=================================================='
	
	end try
	begin catch
		print'============================================='
		print 'Error occured during loading Silver  '
		print 'Error Message' + error_message();
		print 'Error Message' + cast (error_number() as nvarchar);
		print'============================================='

	end catch

end

