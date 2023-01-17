-- Present total sales of all products supplied by each supplier with respect to quarter and month
select s.SUPPLIER_NAME,p.PRODUCT_NAME,
t.Quater ,t.Month , sum(Total_Sale)
From metrowarehouse.fact_table ss 
join metrowarehouse.supplier s 
join metrowarehouse.DATE_TIME t
join metrowarehouse.product p 
on (s.SUPPLIER_ID = ss.SUPPLIER_ID 
		AND ss.Time_Id = t.Time_Id
        AND ss.PRODUCT_ID=p.PRODUCT_ID)
group by SUPPLIER_NAME,Quater,PRODUCT_NAME,Month;

-- Present total sales of each product sold by each store. The output should be organised store wise and then product wise under each store.
select sum(Total_Sale), s.STORE_NAME ,p.PRODUCT_NAME
From metrowarehouse.fact_table ss 
join metrowarehouse.STORE s
join metrowarehouse.PRODUCT p 
on ( ss.PRODUCT_ID=p.PRODUCT_ID AND s.STORE_ID = ss.STORE_ID )
group by STORE_NAME,PRODUCT_NAME;

-- Find the 5 most popular products sold over the weekends.
select sum(Total_Sale),d.day ,p.PRODUCT_NAME
From metrowarehouse.PRODUCT p join metrowarehouse.fact_table ss join metrowarehouse.DATE_TIME d
on ( ss.PRODUCT_ID=p.PRODUCT_ID AND ss.TIME_ID = d.TIME_ID)
where ( day = 'SUNDAY'|| day = 'SATURDAY') 
group by day,PRODUCT_NAME;


-- Find an anomaly in the data warehouse dataset. write a query to show the anomaly and  explain the anomaly in your project report.

select * from PRODUCT where (PRODUCT_NAME = "Tomatoes");

 -- Create a materialised view with name “STOREANALYSIS_MV” that presents the product- wise sales analysis for each store.
 
 -- drop view if exists analysis;
 Create view analysis AS 
 SELECT PRODUCT_ID, STORE_ID, ROUND(SUM(SUM),2) AS TOTAL
 FROM fact_table 
 NATURAL JOIN PRODUCT
 NATURAL JOIN STORE
 group by PRODUCT_ID, STORE_ID;
 select * FROM analysis;

