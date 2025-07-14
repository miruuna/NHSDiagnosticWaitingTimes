DROP TABLE diagnostic_wait_times_db;

CREATE TABLE [dbo].[diagnostic_wait_times_db] (
id INT IDENTITY(1,1) PRIMARY KEY,
    provider_code NVARCHAR(50),
    diagnostic_id INT,
    percentage_over_6weeks FLOAT,
    year INT,
    month NVARCHAR(20),
    region_name NVARCHAR(50),
    provider_name NVARCHAR(50)
);