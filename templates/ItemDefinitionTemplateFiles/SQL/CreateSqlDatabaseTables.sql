DROP TABLE IF EXISTS [dbo].[Products];
CREATE TABLE [dbo].[Products] (
  [ProductId] [int] PRIMARY KEY NOT NULL,
  [Product] [varchar](50) NOT NULL,
  [Category] [varchar](50) NOT NULL
);

DROP TABLE IF EXISTS [dbo].[Customers];
CREATE TABLE [dbo].[Customers] (
  [CustomerId] [int] PRIMARY KEY NOT NULL,
  [FirstName] [varchar](50) NOT NULL,
  [LastName] [varchar](50) NOT NULL,
  [Country] [varchar](50) NOT NULL,
  [City] [varchar](50) NOT NULL,
  [DOB] [date] NULL
);

DROP TABLE IF EXISTS [dbo].[Invoices];
CREATE TABLE [dbo].[Invoices] (
  [InvoiceId] [int] PRIMARY KEY NOT NULL,
  [Date] [date] NOT NULL,
  [TotalSalesAmount] [decimal] NOT NULL,
  [CustomerId] [int] NOT NULL,
  [ProductId] [int] NOT NULL
);

DROP TABLE IF EXISTS [dbo].[InvoiceDetails];
CREATE TABLE [dbo].[InvoiceDetails] (
  [Id] [int] PRIMARY KEY NOT NULL,
  [Quantity] [int] NOT NULL,
  [SalesAmount] [decimal] NOT NULL,
  [ProductId] [int] NOT NULL,
  [InvoiceId] [int] NOT NULL
);
