# Microsoft Fabric Logging Framework

A simple, single-file logging framework for Microsoft Fabric that automatically creates monitoring infrastructure with semantic models and relationships.

## Quick Start

1. **Copy the file**: Download `fabric_logging_utils.py` and upload it to your Fabric Notebook Resources Folder
2. **Import and use**: Start logging your operations immediately

<img width="704" height="454" alt="image" src="https://github.com/user-attachments/assets/83436910-3379-4e16-8e8e-fb12e5ba8f94" />


```python
# Import the framework
import builtin.fabric_logging_utils as fabric_logging_utils
from builtin.fabric_logging_utils import FabricLogger

# Create logger (automatically sets up everything)
logger = FabricLogger("MyProject")

# Log operations
logger.log_operation(
    notebook_name="DataProcessing",
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

## What It Creates Automatically

- **Lakehouse**: `LH_{ProjectName}_Monitoring`
- **Tables**:
  - `monitoring_log` - Main fact table with all operations
  - `dim_date` - Date dimension (4 years of dates)
  - `dim_time` - Time dimension (hourly breakdown)
- **Semantic Model**: `SM_{ProjectName}_Monitoring` with:
  - Relationships between fact and dimension tables
  - 8 pre-built DAX measures for monitoring

## Key Features

- **One file**: No complex installation required
- **Data preservation**: Never overwrites existing data
- **Auto-recovery**: Built-in timing safeguards for Fabric consistency
- **Complete BI solution**: Ready-to-use Power BI semantic model
- **Smart setup**: Only creates what doesn't exist

## Core Methods

### Basic Logging
```python
logger.log_operation(
    notebook_name="NotebookName",
    table_name="TableName", 
    operation_type="INSERT|UPDATE|DELETE|MERGE",
    rows_before=1000,
    rows_after=1200,
    execution_time=5.2,
    message="Operation details",
    error_message="Error if any"
)
```

### Monitoring
```python
logger.show_recent(10)           # Show last 10 operations
logger.get_statistics()          # Show summary stats
logger.show_complete_status()    # Full framework status
```

### Advanced
```python
# If semantic model creation fails during setup
logger.create_semantic_model_when_ready()

# Add relationships and measures to existing model
logger.enhance_semantic_model()

# Filter logs
df = logger.get_logs(table_name="sales_fact", limit=50)
df.show()
```

## DAX Measures Created

The framework automatically creates these measures in your semantic model:

- **Total Operations** - Count of all logged operations
- **Total Rows Changed** - Sum of all row changes
- **Average Execution Time** - Average operation duration
- **Error Count** - Count of failed operations  
- **Success Rate** - Percentage of successful operations
- **Operations Today** - Today's operation count
- **Unique Tables** - Number of distinct tables processed
- **Unique Notebooks** - Number of distinct notebooks run

## Timing Safeguards

The framework includes built-in protections for Fabric's eventual consistency:

- **Table verification** before semantic model creation
- **Retry logic** for failed operations
- **Recovery methods** for timing issues
- **Graceful degradation** when components aren't ready

## Use Cases

- **ETL Monitoring**: Track data pipeline operations
- **Quality Assurance**: Monitor data loads and transformations  
- **Performance Tracking**: Analyze execution times and bottlenecks
- **Error Reporting**: Centralized error tracking across notebooks
- **Compliance**: Audit trail of all data operations

## Requirements

- Microsoft Fabric workspace
- Spark compute
- No additional packages required (auto-installs dependencies)

## File Structure

```
your-workspace/
├── fabric_logging_utils.py    # The complete framework
└── your_notebook.py           # Your code using the framework
```

That's it! Everything you need is in the single file.

## Example Output

```
============================================================
FABRIC LOGGING FRAMEWORK
============================================================

Project Configuration:
  • Project Name: MyProject  
  • Lakehouse: LH_MyProject_Monitoring
  • Semantic Model: SM_MyProject_Monitoring

INITIALIZING FABRIC LOGGING FRAMEWORK
============================================================

Workspace Detection:
  • Workspace ID: 12345...

Lakehouse Setup:
  Found existing lakehouse: LH_MyProject_Monitoring

Table Setup:
  monitoring_log: 150 existing records preserved
  dim_date: 1,461 dates (up to 2027-09-14)
  dim_time: 1,440 time slots (24 hours)

Verifying Tables:
  Checking monitoring_log...
    monitoring_log: 150 rows - Ready
  All tables verified and accessible

Semantic Model Setup:
  Semantic model 'SM_MyProject_Monitoring' already exists
  Tip: Use enhance_semantic_model() to add relationships/measures

============================================================
SETUP COMPLETE
============================================================

Ready to log operations! Use: logger.log_operation(...)
```

## License

MIT License - feel free to use and modify as needed.
