# How to Run

1. Create a Bigtable Instance called `bigtabledevinstance`.
2. Start the Proxy with a listener on port `9042` with a `bigtabledevinstance`
   keyspace configured.
3. Run the tests using typical testing commands like `go test`.

Note: If schema changes are made to any test tables they will need to be
manually deleted and recreated. 

