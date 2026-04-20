## Scalability Analysis

### Current Approach (This Dataset)
The pipeline uses:
- **Window functions** for session attribution (no shuffle explosion for typical visitor volumes)
- **Vectorized pandas UDFs** for URL parsing (~10× faster than row UDFs)
- **Lazy evaluation** throughout — only the final aggregated rows are collected

### For 10 GB+ Files

| Concern | Problem | Solution |
|---|---|---|
| Window over all IPs | Large shuffle if millions of unique IPs | Partition input by date on S3; process one day at a time |
| IP = session proxy | Shared IPs (NAT, offices) pollute attribution | Add user_agent to session key, or use a proper visitor cookie if available |
| Single Glue G.1X worker | OOM on 10 GB | Scale to 10× G.1X workers or use EMR with auto-scaling |
| coalesce(1) on output | Moves all data to one executor | Use partitioned Parquet output instead; post-process with Athena |
| Python UDFs | Serialization overhead per partition | Already using pandas_udf (vectorized); for extreme scale, port to native Spark SQL regex |

### Recommended Architecture at Scale
```
S3 (landing, partitioned by date)
  → AWS Glue job with 10 G.2X workers
  → Adaptive Query Execution enabled
  → Output: partitioned Parquet on S3
  → AWS Athena for ad-hoc querying
  → CloudWatch alarms on job failures
```