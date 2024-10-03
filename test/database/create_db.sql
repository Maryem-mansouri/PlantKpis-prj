-- Create the database
CREATE DATABASE EDI_Data;

-- Switch to the newly created database
USE EDI_Data;

-- Create the table to store the EDI data
CREATE TABLE EDI_Table3 (
    sales_order_id NVARCHAR(255) PRIMARY KEY,
    material_number NVARCHAR(255),
    plant_id NVARCHAR(255),
    order_quantity INT,
    backlog_budget_rate_amount FLOAT,
    customer_request_ship_date DATE,
    customer_request_ship_calendar_week_number INT,
    sold_to_customer_name NVARCHAR(255),
    material_type_name NVARCHAR(255),
    mrp_controller_code NVARCHAR(255),
    backlog_snapshot_date DATE,
    backlog_snapshot_calendar_week_number INT,
    backlog_snapshot_calendar_quarter_number INT,
    trade_intercompany_label NVARCHAR(255),
    calendar_year INT,
    year INT,
    calendar_week_number NVARCHAR(255),
    Team NVARCHAR(255),
    Project NVARCHAR(255)
);


USE Plantkpis_Sp_db;

-- Alter columns to be non-nullable
ALTER TABLE EDI_Table
ALTER COLUMN sales_order_id NVARCHAR(255) NOT NULL;

ALTER TABLE EDI_Table
ALTER COLUMN material_number NVARCHAR(255) NOT NULL;

-- Switch to the newly created database
USE Plantkpis_Sp_db;

-- Create the table to store the EDI data
CREATE TABLE EDI_Table2 (
    sales_order_id NVARCHAR(255) PRIMARY KEY,
    material_number NVARCHAR(255),
    plant_id NVARCHAR(255),
    order_quantity INT,
    backlog_budget_rate_amount FLOAT,
    customer_request_ship_date DATE,
    customer_request_ship_calendar_week_number INT,
    sold_to_customer_name NVARCHAR(255),
    material_type_name NVARCHAR(255),
    mrp_controller_code NVARCHAR(255),
    backlog_snapshot_date DATE,
    backlog_snapshot_calendar_week_number INT,
    backlog_snapshot_calendar_quarter_number INT,
    trade_intercompany_label NVARCHAR(255),
    calendar_year INT,
    year INT,
    calendar_week_number NVARCHAR(255)
);
USE Plantkpis_Sp_db;
CREATE TABLE EDI_Table3 (
    EDI_id INT IDENTITY(1,1) PRIMARY KEY,
    sales_order_id VARCHAR(50),
    material_number NVARCHAR(255),
    plant_id NVARCHAR(255),
    order_quantity INT,
    backlog_budget_rate_amount FLOAT,
    customer_request_ship_date DATE,
    customer_request_ship_calendar_week_number INT,
    sold_to_customer_name NVARCHAR(255),
    material_type_name NVARCHAR(255),
    mrp_controller_code NVARCHAR(255),
    backlog_snapshot_date DATE,
    backlog_snapshot_calendar_week_number INT,
    backlog_snapshot_calendar_quarter_number INT,
    trade_intercompany_label NVARCHAR(255),
    calendar_year INT,
    year INT,
    calendar_week_number NVARCHAR(255)
	Team NVARCHAR(255),
    Project NVARCHAR(255)
);

-- new database structure -------------------------------------------------
USE Plantkpis_Sp_db
-- Create partition function
CREATE PARTITION FUNCTION PF_Year (int)
AS RANGE LEFT FOR VALUES (2023, 2024);

-- Create partition scheme
CREATE PARTITION SCHEME PS_Year
AS PARTITION PF_Year
ALL TO ([PRIMARY]);

-- Create partitioned table
CREATE TABLE EDI_Table4 (
    sales_order_id NVARCHAR(50),
    material_number NVARCHAR(50),
    plant_id NVARCHAR(50),
    order_quantity INT,
    backlog_budget_rate_amount DECIMAL(18, 2),
    customer_request_ship_date DATE,
    customer_request_ship_calendar_week_number INT,
    sold_to_customer_name NVARCHAR(255),
    material_type_name NVARCHAR(255),
    mrp_controller_code NVARCHAR(50),
    backlog_snapshot_date DATE,
    backlog_snapshot_calendar_week_number INT,
    backlog_snapshot_calendar_quarter_number INT,
    trade_intercompany_label NVARCHAR(50),
    calendar_year INT,
    calendar_week_number INT,
    year INT
)
ON PS_Year(calendar_year);

-- Create clustered columnstore index
CREATE CLUSTERED COLUMNSTORE INDEX CCI_EDI_Table4
ON EDI_Table4;

-- Create non-clustered indexes for filtering
CREATE INDEX IX_EDI_Table4_Year_Week
ON EDI_Table4 (calendar_year, calendar_week_number);

CREATE INDEX IX_EDI_Table4_Customer
ON EDI_Table4 (sold_to_customer_name);

CREATE INDEX IX_EDI_Table4_Document
ON EDI_Table4 (sales_order_id);

CREATE INDEX IX_EDI_Table4_Material
ON EDI_Table4 (material_number);

CREATE INDEX IX_EDI_Table4_Ship_Date
ON EDI_Table4 (customer_request_ship_date);

-- View for Customers
CREATE VIEW vw_Customers AS
SELECT DISTINCT sold_to_customer_name
FROM EDI_Table4;

-- View for Documents based on Customer
CREATE VIEW vw_Documents AS
SELECT DISTINCT sales_order_id, sold_to_customer_name
FROM EDI_Table4;

-- View for Materials based on Document
CREATE VIEW vw_Materials AS
SELECT DISTINCT material_number, sales_order_id
FROM EDI_Table4;

-- View for Ship Dates based on Material
CREATE VIEW vw_ShipDates AS
SELECT DISTINCT customer_request_ship_date, material_number
FROM EDI_Table4;
 #----------------------last structure------------------------------
 USE Plantkpis_Sp_db;

-- Create partition function and scheme if they do not exist
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_Year')
BEGIN
    CREATE PARTITION FUNCTION PF_Year (INT)
    AS RANGE LEFT FOR VALUES (2023, 2024);

    CREATE PARTITION SCHEME PS_Year
    AS PARTITION PF_Year ALL TO ([PRIMARY]);
END

-- Create the table
CREATE TABLE EDI_Table4 (
    id_EDI INT IDENTITY(1,1) NOT NULL,
    sales_order_id NVARCHAR(50),
    material_number NVARCHAR(50),
    plant_id NVARCHAR(50),
    order_quantity INT,
    backlog_budget_rate_amount DECIMAL(18, 2),
    customer_request_ship_date DATE,
    customer_request_ship_calendar_week_number INT,
    sold_to_customer_name NVARCHAR(255),
    material_type_name NVARCHAR(255),
    mrp_controller_code NVARCHAR(50),
    backlog_snapshot_date DATE,
    backlog_snapshot_calendar_week_number INT,
    backlog_snapshot_calendar_quarter_number INT,
    trade_intercompany_label NVARCHAR(50),
    calendar_year INT,
    calendar_week_number INT,
    year INT,
    Team NVARCHAR(255),
    Project NVARCHAR(255),
    PRIMARY KEY (id_EDI, calendar_year)
)
ON PS_Year(calendar_year);

-- Create clustered columnstore index
CREATE CLUSTERED COLUMNSTORE INDEX CCI_EDI_Table4
ON EDI_Table4;

-- Create non-clustered indexes for filtering
CREATE INDEX IX_EDI_Table4_Year_Week
ON EDI_Table4 (calendar_year, calendar_week_number);

CREATE INDEX IX_EDI_Table4_Customer
ON EDI_Table4 (sold_to_customer_name);

CREATE INDEX IX_EDI_Table4_Document
ON EDI_Table4 (sales_order_id);

CREATE INDEX IX_EDI_Table4_Material
ON EDI_Table4 (material_number);

CREATE INDEX IX_EDI_Table4_Ship_Date
ON EDI_Table4 (customer_request_ship_date);
USE Plantkpis_Sp_db;
GO

-- Create view for Customers
CREATE VIEW vw_Customers AS
SELECT DISTINCT sold_to_customer_name
FROM dbo.EDI_Table4;
GO

-- Create view for Documents based on Customer
CREATE VIEW vw_Documents AS
SELECT DISTINCT sales_order_id, sold_to_customer_name
FROM dbo.EDI_Table4;
GO

-- Create view for Materials based on Document
CREATE VIEW vw_Materials AS
SELECT DISTINCT material_number, sales_order_id
FROM dbo.EDI_Table4;
GO

-- Create view for Ship Dates based on Material
CREATE VIEW vw_ShipDates AS
SELECT DISTINCT customer_request_ship_date, material_number
FROM dbo.EDI_Table4;
GO
---------------------------------
CREATE VIEW OrderedInventoryView AS
SELECT *, 
       ROW_NUMBER() OVER (ORDER BY Week ASC, Day ASC) AS RowOrder
FROM InventoryTable;
