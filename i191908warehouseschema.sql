-- drop schema if exists metrowh;
-- CREATE schema  metrowh;
USE `metrowh`;
drop table if exists fact_table;
drop table if exists PRODUCT;
drop table if exists CUSTOMER;
drop table if exists STORE;
drop table if exists SUPPLIER;
drop table if exists DATE_TIME;


CREATE TABLE `PRODUCT` (
  `PRODUCT_ID` VARCHAR(6) NOT NULL , 
  `PRODUCT_NAME` VARCHAR(255) NOT NULL,
  primary key(`PRODUCT_ID`));
CREATE TABLE `CUSTOMER` (
  `CUSTOMER_ID` VARCHAR(6) NOT NULL, 
  `CUSTOMER_NAME` VARCHAR(255) NOT NULL,
  primary key(`CUSTOMER_ID`));
CREATE TABLE `STORE` (
  `STORE_ID` VARCHAR(6) NOT NULL, 
  `STORE_Name` VARCHAR(255) NOT NULL,
  primary key(`STORE_ID`));
CREATE TABLE `SUPPLIER` (
  `SUPPLIER_ID` VARCHAR(6) NOT NULL, 
  `SUPPLIER_Name` VARCHAR(255) NOT NULL,
  primary key(`SUPPLIER_ID`));
CREATE TABLE `DATE_TIME` (
  `Time_Id` VARCHAR(25) NOT NULL,
  `Day` VARCHAR(6) NOT NULL,
  `Week` VARCHAR(10) NOT NULL,
  `Month` VARCHAR(10) NOT NULL,
  `Quater` VARCHAR(10) NOT NULL,
  `Year` VARCHAR(4) NOT NULL,
  primary key(`Time_Id`));
  
  CREATE TABLE `fact_table` (
  Transaction_ID Integer NOT NULL,
  primary key(`Transaction_ID`),
  PRODUCT_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID),
  CUSTOMER_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER(CUSTOMER_ID),
  STORE_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (STORE_ID) REFERENCES STORE(STORE_ID),
  SUPPLIER_ID VARCHAR(6) NOT NULL,
   FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID),
  Time_Id VARCHAR(25) NOT NULL,
  FOREIGN KEY (Time_Id) REFERENCES DATE_TIME(Time_Id),
  Total_Sale Float NOT NULL
  );
  