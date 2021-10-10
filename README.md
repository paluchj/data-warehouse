## data-warehouse

Simple Spring Boot application written in Groovy with MongoDB (embedded) as a data store.
Application exposes data extracted from a csv file via REST API.

### Endpoints:

* `/warehouse/load_default` - POST mapping; loads default data (from an embedded csv file in the resource folder) to the data store; returns information with count of loaded documents
* `/warehouse/load` - POST mapping; loads data from passed csv file to the data store; returns information with count of loaded documents
* `/warehouse` - POST mapping, used for query data; returns json with calculated data; consumes parameters:
    * `aggregate` - set of metrics to be aggregated on (mandatory)
    * `group` - set of dimensions to be grouped by (optional)
    * `filter` - MongoDb filter expression to narrow data (optional). More information about filters: [Query Documents](https://docs.mongodb.com/manual/tutorial/query-documents/) and [Aggregation Operators](https://docs.mongodb.com/manual/reference/operator/aggregation/). Examples:
        ```
        { Campaign: "Adventmarkt Touristik" }
        { Datasource: { $ne: "Google Ads"} }
        ```