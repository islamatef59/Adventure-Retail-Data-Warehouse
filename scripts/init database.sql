if EXISTS(SELECT 1 FROM sys.databases WHERE  name='DataWarehouse')
BEGIN 
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END 
GO
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse
GO

CREATE schema bronze;
Go
CREATE schema silver;
Go
CREATE schema Gold;
Go

ALTER DATABASE [DataWarehouse] SET ONLINE;
SELECT name, state_desc 
FROM sys.databases
WHERE name = 'DataWarehouse';

