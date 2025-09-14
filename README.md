# Microsoft Fabric Logging Framework v3.3.0

A simple, single-file logging framework for Microsoft Fabric that automatically creates monitoring infrastructure with semantic models and relationships.

## Quick Start

1. **Copy the file**: Download `fabric_logging_utils.py` and upload it to your Fabric workspace's `builtin` folder
2. **Import and use**: Start logging your operations immediately

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
  - `dim_date` - Date dimension (4 years of dates) with proper DateTime types
  - `dim_time` - Time dimension (hourly breakdown)
- **Semantic Model**: `SM_{ProjectName}_Monitoring` with:
  - Relationships between fact and dimension tables
  - 8 pre-built DAX measures organized in folders
  - Proper data types for all columns

## New in v3.3.0

### Enhanced Semantic Model Features
- **Measure Folders**: Automatically organizes measures into logical folders:
  - **Core Metrics**: Total Operations, Total Rows Changed, Unique Tables, Unique Notebooks
  - **Performance Metrics**: Average Execution Time
  - **Quality Metrics**: Error Count, Success Rate
  - **Time Intelligence**: Operations Today

### Advanced Data Type Configuration
- Proper DateTime formatting for date columns
- Numeric formatting with thousands separators
- Relationship compatibility between string and date types
- Python.NET 3.0 compatibility fixes

### Historical Data Support
- Custom timestamp support for creating historical test data
- Time series data generation for realistic monitoring patterns
- Business hour simulation and date distribution

## Key Features

- **One file**: No complex installation required
- **Data preservation**: Never overwrites existing data
- **Auto-recovery**: Built-in timing safeguards for Fabric consistency
- **Complete BI solution**: Ready-to-use Power BI semantic model with organized measures
- **Smart setup**: Only creates what doesn't exist
- **Historical data**: Support for backdated entries and test data generation

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
    error_message="Error if any",
    custom_timestamp=datetime(2025, 8, 15, 14, 30)  # Optional: for historical data
)
```

### Monitoring
```python
logger.show_recent(10)           # Show last 10 operations
logger.get_statistics()          # Show summary stats
logger.show_complete_status()    # Full framework status
```

### Advanced Semantic Model Management
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

The framework automatically creates these measures organized in folders:

### Core Metrics
- **Total Operations** - Count of all logged operations
- **Total Rows Changed** - Sum of all row changes  
- **Unique Tables** - Number of distinct tables processed
- **Unique Notebooks** - Number of distinct notebooks run

### Performance Metrics
- **Average Execution Time** - Average operation duration in seconds

### Quality Metrics
- **Error Count** - Count of failed operations
- **Success Rate** - Percentage of successful operations

### Time Intelligence
- **Operations Today** - Today's operation count

## Data Type Configuration

The framework automatically configures proper data types:

- **Date columns**: DateTime format with calendar icons
- **Numeric columns**: Proper formatting with thousands separators
- **String columns**: Text data type
- **Boolean columns**: True/false values
- **Relationship columns**: Compatible types for proper joins

## Timing Safeguards

Built-in protections for Fabric's eventual consistency:

- **Table verification** before semantic model creation
- **Retry logic** for failed operations
- **Recovery methods** for timing issues
- **Graceful degradation** when components aren't ready

## Test Data Generation

Create realistic test data with historical patterns:

```python
# Import test data utilities
from builtin.test_data_creation import setup_complete_test_environment, quick_test

# Quick test with minimal data
logger = quick_test("TestProject")

# Comprehensive test environment with historical data
logger = setup_complete_test_environment("TestProject")
```

## Use Cases

- **ETL Monitoring**: Track data pipeline operations with proper time intelligence
- **Quality Assurance**: Monitor data loads and transformations  
- **Performance Tracking**: Analyze execution times and bottlenecks
- **Error Reporting**: Centralized error tracking across notebooks
- **Compliance**: Audit trail of all data operations
- **Historical Analysis**: Time-based reporting with proper date dimensions

## Requirements

- Microsoft Fabric workspace
- Spark compute
- No additional packages required (auto-installs dependencies)

## File Structure

```
your-workspace/
├── builtin/
│   ├── fabric_logging_utils.py     # The complete framework
│   ├── usage_examples.py           # Comprehensive usage examples
│   └── test_data_creation.py       # Test data generation utilities
└── your_notebook.py                # Your code using the framework
```

## Example Output

```
============================================================
FABRIC LOGGING FRAMEWORK v3.3.0
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
  Configuring data types and formatting...
  Marked dim_date as date table
  Data types and formatting configured successfully
  Updated: Total Operations -> Core Metrics
  Updated: Average Execution Time -> Performance Metrics
  Summary: Created 0, Updated 8

============================================================
SETUP COMPLETE
============================================================

Ready to log operations! Use: logger.log_operation(...)
```

## Troubleshooting

### Common Issues

**Python.NET Enum Errors**: The framework automatically handles Python.NET 3.0 compatibility by using proper DataType enums.

**Relationship Compatibility**: Date relationships use compatible string types while maintaining DateTime functionality through separate columns.

**Missing Measures**: Use `logger.enhance_semantic_model()` to add relationships and measures to existing models.

**Tables Not Ready**: The framework includes retry logic and table verification. Use `logger.create_semantic_model_when_ready()` if needed.

### Getting Help

- Check `logger.show_complete_status()` for framework health
- Use `logger.get_statistics()` for current data summary
- Enable detailed logging to see configuration steps

## Version History

- **v3.3.0** - Enhanced semantic model with measure folders, proper data types, and historical data support
- **v3.2.0** - Added timing safeguards and recovery methods
- **v3.1.0** - Initial semantic model automation
- **v3.0.0** - Complete framework with relationship management

## License

MIT License - feel free to use and modify as needed.
