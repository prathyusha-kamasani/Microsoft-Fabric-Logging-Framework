# Microsoft Fabric Logging Framework

A production-ready, single-file logging framework for Microsoft Fabric that automatically creates a comprehensive monitoring infrastructure with semantic models, relationships, and Power BI integration. This is a hobby project next to my life and work, so I have used AI to create this Read Me file. I did do fact checks, but please use them by keeping that in mind. 

## Quick Start

### 1. Copy the File
Download `fabric_logging_utils.py` and upload it to your Fabric Notebook Resources Folder

### 2. Import and Use
Start logging your operations immediately:

```python
# Import the framework
import builtin.fabric_logging_utils as fabric_logging_utils

# Create logger (automatically sets up everything)
logger = fabric_logging_utils.FabricLogger("MyProject")

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

## What Gets Created Automatically

### Infrastructure
- **Lakehouse**: `LH_{ProjectName}_Monitoring`
- **Tables**:
  - `monitoring_log` - Main fact table with all operations
  - `dim_date` - Date dimension (4 years of dates) 
  - `dim_time` - Time dimension (hourly breakdown)
- **Semantic Model**: `SM_{ProjectName}_Monitoring` with:
  - Relationships between fact and dimension tables
  - 8 pre-built DAX measures for monitoring
  - Power BI ready analytics

## Key Features

- **One file**: No complex installation required
- **Data preservation**: Never overwrites existing data
- **Auto-recovery**: Built-in timing safeguards for Fabric consistency
- **Complete BI solution**: Ready-to-use Power BI semantic model
- **Smart setup**: Only creates what doesn't exist
- **Automatic authentication**: Uses Fabric native tokens for reliability

## Core Operations

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

### Monitoring & Analysis
```python
logger.show_recent(10)              # Show last 10 operations
logger.get_statistics()             # Show summary stats
logger.show_complete_status()       # Full framework status

# Filter logs
df = logger.get_logs(table_name="sales_fact", limit=50)
df.show()
```

### Advanced Operations
```python
# If semantic model creation fails during setup
logger.create_semantic_model_when_ready()

# Add relationships and measures to existing model
logger.enhance_semantic_model()

# Force recreate everything (will lose data)
logger = FabricLogger("MyProject", force_recreate=True)
```

## Built-in DAX Measures

The framework automatically creates these measures in your semantic model:

- **Total Operations** - Count of all logged operations
- **Total Rows Changed** - Sum of all row changes  
- **Average Execution Time** - Average operation duration
- **Error Count** - Count of failed operations
- **Success Rate** - Percentage of successful operations
- **Operations Today** - Today's operation count
- **Unique Tables** - Number of distinct tables processed
- **Unique Notebooks** - Number of distinct notebooks run

## Fabric Consistency Protection

The framework includes built-in protections for Fabric's eventual consistency:

- Table verification before semantic model creation
- Retry logic for failed operations
- Recovery methods for timing issues
- Graceful degradation when components aren't ready
- Smart authentication using Fabric native tokens

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

## Installation

Simple file structure:
```
your-workspace/
├── fabric_logging_utils.py    # The complete framework
└── your_notebook.py          # Your code using the framework
```

## Version 3.4.3 Updates (September 2025)

### Fixed Authentication Issues
- Resolved TOM library authentication problems using Fabric native tokens
- Fixed `super(type, obj): obj must be an instance or subtype of type` errors  
- Improved compatibility with Microsoft Fabric environments

### Enhanced Features
- Automatic relationship and measure updates for existing semantic models
- Smart detection of existing components to avoid unnecessary operations
- Better error handling and fallback instructions
- Enhanced data type configuration for semantic models

### Known Issues & Solutions

#### TOM Operations Fail with Authentication Errors
- **Issue**: `super(type, obj): obj must be an instance or subtype of type`
- **Cause**: Azure Identity library compatibility issues
- **Solution**: Framework now automatically uses Fabric native tokens

#### Existing Semantic Models Won't Update
- **Issue**: Enhancement operations fail on existing models
- **Cause**: Corrupted model state from previous versions
- **Solution**: Create fresh semantic model with new project name

```python
# For new projects (recommended)
logger = FabricLogger("NewProjectName")

# For problematic existing projects  
logger = FabricLogger("ExistingProject", force_recreate=True)
```

### Success Indicators

When working properly, you should see:
```
Setting up Fabric authentication for TOM...
Connecting to semantic model with Fabric auth...
Updated: Total Operations
Summary: Created 0, Updated 8
Semantic model refreshed successfully
```

## Example Output

```
============================================================
FABRIC LOGGING FRAMEWORK v3.4.3
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

## Version History

### v3.4.3 (September 2025)
- Fixed authentication issues with TOM operations
- Enhanced semantic model update capabilities  
- Improved error handling and recovery
- Added support for measure and relationship updates
- Better handling of existing vs new semantic models

### v3.3.0 (Previous)
- Basic semantic model creation
- Core logging functionality
- Table management and verification
- Initial TOM integration

## Troubleshooting

### Framework Won't Initialize
Check that you're in a Microsoft Fabric notebook environment with Spark compute available.

### Tables Not Creating
Ensure you have sufficient permissions in the workspace and lakehouse creation rights.

### Semantic Model Issues
Try using a fresh project name or set `force_recreate=True` to rebuild components.

### TOM Authentication Failures
Version 3.4.3 includes fixes for authentication issues. Ensure you're using the latest version.

## License

MIT License - feel free to use and modify as needed.

## Contributing

This is a single-file framework designed for simplicity. For issues or enhancements, please create GitHub issues with detailed descriptions and error messages.
