[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared CSV_FILE_ROOT = "https://fabricdevcamp.blob.core.windows.net/sampledata/ProductSales/Dev/" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
[BindToDefaultDestination = true]
shared Products = let
    Source = Csv.Document(Web.Contents(CSV_FILE_ROOT, [RelativePath="Products.csv"]),[Delimiter=",", Columns=3, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders,{{"ProductId", Int64.Type}, {"Product", type text}, {"Category", type text}})
in
    ChangedType;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "21d4a8b3-ef7d-417e-9170-a87c1421dd31"]}[Data]{[lakehouseId = "3a41e028-1ff4-4048-8f61-57245ffeb314"]}[Data];
