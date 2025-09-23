[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]

section Section1;

shared CSV_FILE_ROOT = "https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev/" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];

[BindToDefaultDestination = true]

shared Products = let
    Source = Csv.Document(Web.Contents(CSV_FILE_ROOT, [RelativePath="Products.csv"]),[Delimiter=",", Columns=3, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(PromotedHeaders,{{"ProductId", Int64.Type}, {"Product", type text}, {"Category", type text}})
in
    #"Changed Type";

[BindToDefaultDestination = true]

shared Customers = let
    Source = Csv.Document(Web.Contents(CSV_FILE_ROOT, [RelativePath="Customers.csv"]),[Delimiter=",", Columns=6, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Columns" = Table.CombineColumns(PromotedHeaders,{"FirstName", "LastName"},Combiner.CombineTextByDelimiter(" ", QuoteStyle.None),"Customer"),
    #"Renamed Columns" = Table.RenameColumns(#"Merged Columns",{{"City", "City Name"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "City Name", "City Name - Copy"),
    #"Duplicated Column1" = Table.DuplicateColumn(#"Duplicated Column", "Country", "Country - Copy"),
    #"Merged Columns1" = Table.CombineColumns(#"Duplicated Column1",{"City Name - Copy", "Country - Copy"},Combiner.CombineTextByDelimiter(", ", QuoteStyle.None),"City")
in 
      #"Merged Columns1";

[BindToDefaultDestination = true]

shared Sales = let
    Source = Csv.Document(Web.Contents(CSV_FILE_ROOT, [RelativePath="InvoiceDetails.csv"]),[Delimiter=",", Columns=5, Encoding=65001,QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Queries" = Table.NestedJoin(PromotedHeaders, {"InvoiceId"}, Invoices, {"InvoiceId"}, "Invoices", JoinKind.LeftOuter),
    #"Expanded Invoices" = Table.ExpandTableColumn(#"Merged Queries", "Invoices", {"Date", "CustomerId"}, {"Date", "CustomerId"}),
    #"Removed Columns" = Table.RemoveColumns(#"Expanded Invoices",{"InvoiceId"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"ProductId", Int64.Type}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"SalesAmount", "Sales"}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns",{{"Id", Int64.Type}, {"Quantity", Int64.Type}, {"Sales", Currency.Type}, {"Date", type date}})
in 
    #"Changed Type1";

[BindToDefaultDestination = true]

shared Invoices = let
    Source = Csv.Document(Web.Contents(CSV_FILE_ROOT, [RelativePath="Invoices.csv"]),[Delimiter=",", Columns=4, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(PromotedHeaders,{{"Date", type date}, {"TotalSalesAmount", Currency.Type}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type",{"TotalSalesAmount"})
in 
    #"Removed Columns";

