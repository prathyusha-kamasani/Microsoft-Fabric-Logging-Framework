# usage_examples.py
# Complete examples of how to use the Fabric Logging Framework
# Copy these examples to your Fabric notebooks

# First, import the framework
from fabric_logging_utils import FabricLogger, time_operation

# =============================================================================
# EXAMPLE 1: Basic Setup and Usage
# =============================================================================

# Create logger - this sets up everything automatically
logger = FabricLogger("CustomerAnalytics")

# Log a simple operation
logger.log_operation(
    notebook_name="DataIngestion",
    table_name="customers",
    operation_type="INSERT",
    rows_before=0,
    rows_after=10000,
    execution_time=12.5,
    message="Initial customer load from CRM"
)

# Log an error
logger.log_operation(
    notebook_name="DataValidation", 
    table_name="orders",
    operation_type="VALIDATE",
    rows_before=5000,
    rows_after=4850,
    execution_time=3.2,
    error_message="150 records failed validation - missing customer_id",
    message="Validation found data quality issues"
)

# =============================================================================
# EXAMPLE 2: Using the Time Decorator
# =============================================================================

@time_operation
def load_sales_data():
    """Example function that loads sales data"""
    # Simulate data loading
    import time
    time.sleep(2)  # Simulate processing
    return {"rows_loaded": 5000, "status": "success"}

# Execute and log the operation
result, execution_time, error = load_sales_data()

if error:
    logger.log_operation(
        notebook_name="SalesETL",
        table_name="sales_fact",
        operation_type="LOAD",
        execution_time=execution_time,
        error_message=error
    )
else:
    logger.log_operation(
        notebook_name="SalesETL", 
        table_name="sales_fact",
        operation_type="LOAD",
        rows_before=0,
        rows_after=result["rows_loaded"],
        execution_time=execution_time,
        message=f"Sales data loaded successfully: {result['status']}"
    )

# =============================================================================
# EXAMPLE 3: Monitoring and Reporting
# =============================================================================

# View recent operations
print("Recent Operations:")
logger.show_recent(5)

# Get statistics
stats = logger.get_statistics()
print(f"\nCurrent Stats: {stats}")

# Get filtered logs
print("\nError Operations:")
error_logs = logger.get_logs(operation_type="VALIDATE")
error_logs.show()

# Show complete status
logger.show_complete_status()

# =============================================================================
# EXAMPLE 4: ETL Pipeline Monitoring
# =============================================================================

def complete_etl_pipeline():
    """Example of monitoring a complete ETL pipeline"""
    
    # Stage 1: Extract
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="source_system",
        operation_type="EXTRACT",
        rows_before=0,
        rows_after=50000,
        execution_time=45.2,
        message="Extracted data from source system"
    )
    
    # Stage 2: Transform
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="staging_table", 
        operation_type="TRANSFORM",
        rows_before=50000,
        rows_after=49500,
        execution_time=120.7,
        message="Applied business rules and transformations"
    )
    
    # Stage 3: Load
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="data_warehouse",
        operation_type="LOAD",
        rows_before=1000000,
        rows_after=1049500,
        execution_time=85.3,
        message="Loaded transformed data to warehouse"
    )
    
    print("ETL Pipeline completed and logged")

# Run the pipeline
complete_etl_pipeline()

# =============================================================================
# EXAMPLE 5: Data Quality Monitoring
# =============================================================================

def monitor_data_quality():
    """Example of data quality monitoring"""
    
    # Read your data (example)
    # df = spark.read.table("your_table")
    
    # Simulate data quality checks
    total_records = 10000
    null_records = 150
    duplicate_records = 25
    
    # Log data quality metrics
    logger.log_operation(
        notebook_name="DataQuality",
        table_name="customer_master",
        operation_type="QUALITY_CHECK",
        rows_before=total_records,
        rows_after=total_records - null_records - duplicate_records,
        execution_time=8.5,
        message=f"Quality check: {null_records} nulls, {duplicate_records} duplicates removed"
    )
    
    # Log specific quality issues if found
    if null_records > 100:
        logger.log_operation(
            notebook_name="DataQuality",
            table_name="customer_master", 
            operation_type="QUALITY_ALERT",
            execution_time=0.1,
            error_message=f"High null count detected: {null_records} records",
            message="Data quality threshold exceeded"
        )

monitor_data_quality()

# =============================================================================
# EXAMPLE 6: Incremental Load Monitoring
# =============================================================================

def incremental_load_example():
    """Example of monitoring incremental loads"""
    
    from datetime import datetime
    
    # Get current counts (simulate)
    current_count = 1000000
    new_records = 5000
    updated_records = 200
    
    # Log the incremental load
    logger.log_operation(
        notebook_name="IncrementalLoad",
        table_name="sales_transactions",
        operation_type="INCREMENTAL_LOAD", 
        rows_before=current_count,
        rows_after=current_count + new_records,
        execution_time=15.7,
        message=f"Incremental load: {new_records} new, {updated_records} updated"
    )
    
    # Log merge operation
    logger.log_operation(
        notebook_name="IncrementalLoad",
        table_name="sales_transactions",
        operation_type="MERGE",
        rows_before=current_count + new_records,
        rows_after=current_count + new_records,  # No net change after merge
        execution_time=22.3,
        message=f"Merged changes: {updated_records} records updated in place"
    )

incremental_load_example()

# =============================================================================
# EXAMPLE 7: Advanced Semantic Model Management
# =============================================================================

# If semantic model creation failed during initial setup, retry it
if not logger._semantic_model_exists():
    print("Semantic model not found, creating it...")
    success = logger.create_semantic_model_when_ready(max_wait_minutes=3)
    if success:
        print("Semantic model created successfully!")
    else:
        print("Could not create semantic model - check table readiness")

# Add relationships and measures to existing model
logger.enhance_semantic_model()

# =============================================================================
# EXAMPLE 8: Batch Processing Monitoring
# =============================================================================

def batch_processing_example():
    """Example of monitoring batch processing jobs"""
    
    batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Process multiple tables in batch
    tables_to_process = [
        {"name": "customers", "records": 50000},
        {"name": "orders", "records": 200000}, 
        {"name": "products", "records": 10000}
    ]
    
    for table_info in tables_to_process:
        table_name = table_info["name"]
        record_count = table_info["records"]
        
        # Simulate processing time based on record count
        processing_time = record_count / 10000  # Rough estimate
        
        logger.log_operation(
            notebook_name="BatchProcessor",
            table_name=table_name,
            operation_type="BATCH_PROCESS",
            rows_before=0,
            rows_after=record_count,
            execution_time=processing_time,
            message=f"Batch {batch_id} - Processed {table_name}"
        )
    
    # Log batch completion
    logger.log_operation(
        notebook_name="BatchProcessor",
        table_name="batch_control",
        operation_type="BATCH_COMPLETE",
        execution_time=sum(table["records"] for table in tables_to_process) / 10000,
        message=f"Batch {batch_id} completed - {len(tables_to_process)} tables processed"
    )

batch_processing_example()

# =============================================================================
# EXAMPLE 9: Error Handling and Recovery
# =============================================================================

def error_handling_example():
    """Example of proper error handling with logging"""
    
    try:
        # Simulate an operation that might fail
        # df = spark.read.table("might_not_exist")
        raise Exception("Table not found: source_data")
        
    except Exception as e:
        # Log the error
        logger.log_operation(
            notebook_name="ErrorHandling",
            table_name="source_data",
            operation_type="READ",
            execution_time=0.5,
            error_message=str(e),
            message="Failed to read source table"
        )
        
        # Log recovery action
        logger.log_operation(
            notebook_name="ErrorHandling", 
            table_name="backup_source",
            operation_type="FALLBACK",
            rows_after=25000,
            execution_time=5.2,
            message="Switched to backup data source after primary failure"
        )

error_handling_example()

# =============================================================================
# EXAMPLE 10: Performance Monitoring
# =============================================================================

def performance_monitoring_example():
    """Example of monitoring performance across operations"""
    
    # Log different operation types with varying performance
    operations = [
        {"type": "SELECT", "time": 2.1, "rows": 50000},
        {"type": "JOIN", "time": 15.7, "rows": 75000},
        {"type": "AGGREGATE", "time": 8.3, "rows": 1000},
        {"type": "SORT", "time": 12.4, "rows": 75000}
    ]
    
    for op in operations:
        logger.log_operation(
            notebook_name="PerformanceTest",
            table_name="performance_test",
            operation_type=op["type"],
            rows_after=op["rows"],
            execution_time=op["time"],
            message=f"Performance test: {op['type']} operation"
        )
    
    # Check if any operations were slow
    slow_threshold = 10.0  # seconds
    for op in operations:
        if op["time"] > slow_threshold:
            logger.log_operation(
                notebook_name="PerformanceTest",
                table_name="performance_alerts",
                operation_type="PERFORMANCE_ALERT",
                execution_time=0.1,
                message=f"Slow operation detected: {op['type']} took {op['time']}s"
            )

performance_monitoring_example()

print("\n" + "="*60)
print("ALL EXAMPLES COMPLETED")
print("="*60)
print("Check your monitoring logs with:")
print("  logger.show_recent(20)")
print("  logger.get_statistics()")
print("  logger.show_complete_status()")
print("="*60)
