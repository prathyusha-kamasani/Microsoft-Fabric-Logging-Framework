# Microsoft Fabric Logging Framework

A single-file logging framework for Microsoft Fabric that automatically creates monitoring infrastructure: lakehouse, tables, and a Power BI semantic model with relationships and measures.

## Quick Start

### 1. Download and Upload

Download `fabric_logging_utils.py` and upload it to your Fabric notebook's **Resources** folder.

### 2. Import and Initialise

```python
from builtin.fabric_logging_utils import FabricLogger

# Creates monitoring infrastructure automatically
logger = FabricLogger("MyProject")
```

### 3. Start Logging

```python
logger.log_operation(
    notebook_name="DailyETL",
    table_name="sales_fact",
    operation_type="INSERT",
    rows_before=1000,
    rows_after=1500,
    execution_time=2.3,
    message="Daily load completed"
)

# View recent activity
logger.show_recent(10)
```

## What Gets Created

When you initialise the logger, it creates:

| Component | Name | Description |
|-----------|------|-------------|
| **Lakehouse** | `LH_{ProjectName}_Monitoring` | Storage for monitoring data |
| **Fact Table** | `monitoring_log` | All logged operations |
| **Date Dimension** | `dim_date` | 4 years of dates with attributes |
| **Time Dimension** | `dim_time` | 1,440 time slots (every minute) |
| **Semantic Model** | `SM_{ProjectName}_Monitoring` | Direct Lake model with relationships and measures |

**Note:** If your notebook has a default lakehouse attached, the framework uses that instead of creating a new one.

## Pre-built DAX Measures

The semantic model includes these measures out of the box:

- **Total Operations** – Count of all logged operations
- **Total Rows Changed** – Sum of row changes
- **Average Execution Time** – Mean operation duration
- **Error Count** – Count of operations with errors
- **Success Rate** – Percentage of successful operations
- **Operations Today** – Today's operation count
- **Unique Tables** – Distinct tables processed
- **Unique Notebooks** – Distinct notebooks run

## Method Reference

### Logging

```python
logger.log_operation(
    notebook_name="NotebookName",
    table_name="TableName",
    operation_type="INSERT",      # INSERT, UPDATE, DELETE, MERGE, VALIDATE, etc.
    rows_before=1000,
    rows_after=1200,
    execution_time=5.2,
    message="Operation details",
    error_message=None            # Populate if operation failed
)
```

### Monitoring

```python
logger.show_recent(10)            # Show last N operations
logger.get_statistics()           # Summary stats
logger.show_complete_status()     # Full framework status

# Get logs as DataFrame with filters
df = logger.get_logs(table_name="sales_fact", limit=50)
```

### Maintenance

```python
logger.cleanup_old_logs(days_to_keep=90)    # Remove old records
logger.enhance_semantic_model()              # Update relationships and measures
logger.create_semantic_model_when_ready()    # Retry semantic model creation
```

### Force Recreate

If you need to start fresh (this will lose existing data):

```python
logger = FabricLogger("MyProject", force_recreate=True)
```

## Use Cases

- **ETL Pipeline Health** – Track which processes completed, failed, or are slow
- **Data Quality Monitoring** – Log row counts at each transformation stage
- **Performance Optimisation** – Identify bottlenecks using execution time data
- **Compliance & Auditing** – Maintain audit trails of all data operations
- **Multi-team Coordination** – Central visibility across teams and notebooks

## Requirements

- Microsoft Fabric workspace with Spark compute
- Notebook with a default lakehouse attached (recommended)
- No additional packages required (dependencies auto-install)

## Troubleshooting

### Import Error: `No module named 'fabric_logging_utils'`

The file must be in the notebook's Resources folder. Import using:

```python
from builtin.fabric_logging_utils import FabricLogger
```

### Semantic Model Not Created

Tables must be fully committed before the semantic model can be created. If it fails during setup:

```python
logger.create_semantic_model_when_ready()
```

### TOM Authentication Errors

Version 3.4.3 uses Fabric native tokens to avoid authentication issues. Ensure you're using the latest version.

## Version History

### v3.4.3 (September 2025)
- Fixed TOM authentication using Fabric native tokens
- Smart detection of existing components
- Enhanced error handling and recovery

### v3.3.0
- Core logging functionality
- Basic semantic model creation
- Table management and verification

## Licence

MIT – use and modify freely.

## Contributing

Issues and suggestions welcome. Please include error messages and steps to reproduce.

---

Built by [Prathy Kamasani](https://www.data-nova.io) | [Blog](https://www.data-nova.io/blog) | [LinkedIn](https://www.linkedin.com/in/prathy/)
