/*
==============================================
Create Databse and Schemas
==============================================
Script purpose:
		This script creates new database named "DataWarehouse" sfter checking it already exists or not.
		It the darabase exists, it is dropped and recreated. Additionally the scripts sets up three
		within the database: 'bronze', 'silver', and 'gold'.

Warning:
		running this scripts will drop the entire 'DataWarehouse', database if exists.
		All data in the database will be permently deleted. preoceed with the caution and
		ensure you have proper backups before running this scripts.
*/

use master;
go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set SINGLE_USER with rollback immediate;
	drop database DataWarehouse;
end;
go

--Create Database

create database DataWarehouse;
go

use DataWarehouse;
go

create schema bronze;
go

create schema gold;
go

create schema silver;
go

