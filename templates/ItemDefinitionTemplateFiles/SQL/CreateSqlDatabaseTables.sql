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
  [TotalSalesAmount] [decimal](18, 2) NOT NULL,
  [CustomerId] [int] NOT NULL,
  CONSTRAINT FK_Invoices_Customers FOREIGN KEY ([CustomerId])
    REFERENCES [dbo].[Customers]([CustomerId])
);

DROP TABLE IF EXISTS [dbo].[InvoiceDetails];
CREATE TABLE [dbo].[InvoiceDetails] (
  [Id] [int] IDENTITY(1,1) PRIMARY KEY,
  [Quantity] [int] NOT NULL,
  [SalesAmount] [decimal](18, 2) NOT NULL,
  [ProductId] [int] NOT NULL,
  [InvoiceId] [int] NOT NULL,
  CONSTRAINT FK_InvoiceDetails_Products FOREIGN KEY ([ProductId])
    REFERENCES [dbo].[Products]([ProductId]),
  CONSTRAINT FK_InvoiceDetails_Invoices FOREIGN KEY ([InvoiceId])
    REFERENCES [dbo].[Invoices]([InvoiceId])
);
