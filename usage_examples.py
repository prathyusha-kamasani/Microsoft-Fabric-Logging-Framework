# usage_examples.py
# Complete examples of how to use the Fabric Logging Framework v3.3.0
# Copy these examples to your Fabric notebooks

# First, import the framework
import builtin.fabric_logging_utils as fabric_logging_utils
from builtin.fabric_logging_utils import FabricLogger, time_operation
from datetime import datetime, timedelta
import time

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
# EXAMPLE 2: Historical Data Logging with Custom Timestamps
# =============================================================================

# Log operations with historical timestamps for test data
historical_date = datetime.now() - timedelta(days=5)

logger.log_operation(
    notebook_name="DataMigration",
    table_name="legacy_data",
    operation_type="MIGRATE",
    rows_before=0,
    rows_after=50000,
    execution_time=45.2,
    message="Legacy data migration completed",
    custom_timestamp=historical_date  # Use custom timestamp
)

# Log a series of historical operations
for i in range(7):
    operation_date = datetime.now() - timedelta(days=i)
    logger.log_operation(
        notebook_name="DailyETL",
        table_name="sales_fact",
        operation_type="REFRESH",
        rows_before=100000,
        rows_after=100000 + (i * 1000),
        execution_time=15.0 + (i * 2),
        message=f"Daily refresh for {operation_date.strftime('%Y-%m-%d')}",
        custom_timestamp=operation_date
    )

# =============================================================================
# EXAMPLE 3: Using the Time Decorator
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
# EXAMPLE 4: Monitoring and Reporting
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

# Show complete status with enhanced features
logger.show_complete_status()

# =============================================================================
# EXAMPLE 5: Enhanced Semantic Model Management
# =============================================================================

# Check if semantic model exists and enhance it
print("\nEnhancing semantic model with measure folders and data types...")
enhancement_result = logger.enhance_semantic_model()

if enhancement_result:
    print("Semantic model enhanced successfully!")
    print("Measures are now organized in folders:")
    print("  - Core Metrics: Total Operations, Total Rows Changed, etc.")
    print("  - Performance Metrics: Average Execution Time")
    print("  - Quality Metrics: Error Count, Success Rate")
    print("  - Time Intelligence: Operations Today")
else:
    print("Enhancement failed. Trying recovery...")
    logger.create_semantic_model_when_ready()

# =============================================================================
# EXAMPLE 6: ETL Pipeline Monitoring with Time Patterns
# =============================================================================

def complete_etl_pipeline_with_timestamps():
    """Example of monitoring a complete ETL pipeline with realistic timestamps"""
    
    # Start time for the pipeline
    pipeline_start = datetime.now() - timedelta(hours=2)
    
    # Stage 1: Extract (2 hours ago)
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="source_system",
        operation_type="EXTRACT",
        rows_before=0,
        rows_after=50000,
        execution_time=45.2,
        message="Extracted data from source system",
        custom_timestamp=pipeline_start
    )
    
    # Stage 2: Transform (1.5 hours ago)
    transform_time = pipeline_start + timedelta(minutes=30)
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="staging_table", 
        operation_type="TRANSFORM",
        rows_before=50000,
        rows_after=49500,
        execution_time=120.7,
        message="Applied business rules and transformations",
        custom_timestamp=transform_time
    )
    
    # Stage 3: Load (1 hour ago)
    load_time = transform_time + timedelta(minutes=45)
    logger.log_operation(
        notebook_name="ETL_Pipeline",
        table_name="data_warehouse",
        operation_type="LOAD",
        rows_before=1000000,
        rows_after=1049500,
        execution_time=85.3,
        message="Loaded transformed data to warehouse",
        custom_timestamp=load_time
    )
    
    print("ETL Pipeline with timestamps completed and logged")

# Run the pipeline
complete_etl_pipeline_with_timestamps()

# =============================================================================
# EXAMPLE 7: Data Quality Monitoring with Enhanced Tracking
# =============================================================================

def monitor_data_quality_enhanced():
    """Example of enhanced data quality monitoring"""
    
    # Simulate data quality checks across multiple time periods
    for days_ago in range(3):
        check_time = datetime.now() - timedelta(days=days_ago)
        
        # Simulate varying data quality
        total_records = 10000
        null_records = 150 + (days_ago * 25)  # Quality degrades over time
        duplicate_records = 25 + (days_ago * 10)
        
        # Log data quality metrics
        logger.log_operation(
            notebook_name="DataQuality",
            table_name="customer_master",
            operation_type="QUALITY_CHECK",
            rows_before=total_records,
            rows_after=total_records - null_records - duplicate_records,
            execution_time=8.5,
            message=f"Quality check: {null_records} nulls, {duplicate_records} duplicates removed",
            custom_timestamp=check_time
        )
        
        # Log quality alerts if thresholds exceeded
        if null_records > 100:
            logger.log_operation(
                notebook_name="DataQuality",
                table_name="customer_master", 
                operation_type="QUALITY_ALERT",
                execution_time=0.1,
                error_message=f"High null count detected: {null_records} records",
                message="Data quality threshold exceeded",
                custom_timestamp=check_time
            )

monitor_data_quality_enhanced()

# =============================================================================
# EXAMPLE 8: Performance Monitoring with Business Hours Simulation
# =============================================================================

def performance_monitoring_business_hours():
    """Example of performance monitoring with business hour patterns"""
    
    # Simulate operations during business hours over the past week
    for day in range(7):
        base_date = datetime.now() - timedelta(days=day)
        
        # Business hours: 9 AM to 5 PM
        for hour in range(9, 17):
            operation_time = base_date.replace(hour=hour, minute=0, second=0)
            
            # Performance varies by time of day
            base_execution_time = 15.0
            if 12 <= hour <= 13:  # Lunch time - slower
                execution_time = base_execution_time * 1.5
            elif hour in [9, 16]:  # Peak hours - slower
                execution_time = base_execution_time * 1.2
            else:
                execution_time = base_execution_time
            
            logger.log_operation(
                notebook_name="PerformanceMonitoring",
                table_name="transaction_processing",
                operation_type="PROCESS",
                rows_before=50000,
                rows_after=50000,
                execution_time=execution_time,
                message=f"Hourly processing at {hour}:00",
                custom_timestamp=operation_time
            )

performance_monitoring_business_hours()

# =============================================================================
# EXAMPLE 9: Error Handling and Recovery with Timeline
# =============================================================================

def error_handling_timeline_example():
    """Example of error handling with recovery timeline"""
    
    # Initial failure
    failure_time = datetime.now() - timedelta(hours=1)
    logger.log_operation(
        notebook_name="ErrorHandling",
        table_name="critical_process",
        operation_type="EXECUTE",
        execution_time=0.5,
        error_message="Connection timeout to source system",
        message="Critical process failed",
        custom_timestamp=failure_time
    )
    
    # Retry attempts
    for attempt in range(3):
        retry_time = failure_time + timedelta(minutes=10 * (attempt + 1))
        
        if attempt < 2:  # First two retries fail
            logger.log_operation(
                notebook_name="ErrorHandling",
                table_name="critical_process",
                operation_type="RETRY",
                execution_time=1.0 + attempt,
                error_message=f"Retry {attempt + 1} failed - connection still unavailable",
                message=f"Retry attempt {attempt + 1}",
                custom_timestamp=retry_time
            )
        else:  # Final retry succeeds
            logger.log_operation(
                notebook_name="ErrorHandling",
                table_name="critical_process",
                operation_type="RETRY",
                rows_before=0,
                rows_after=25000,
                execution_time=5.2,
                message=f"Retry {attempt + 1} successful - connection restored",
                custom_timestamp=retry_time
            )

error_handling_timeline_example()

# =============================================================================
# EXAMPLE 10: Comprehensive Batch Processing with Mixed Results
# =============================================================================

def comprehensive_batch_processing():
    """Example of comprehensive batch processing with varied outcomes"""
    
    batch_start_time = datetime.now() - timedelta(hours=3)
    batch_id = f"BATCH_{batch_start_time.strftime('%Y%m%d_%H%M%S')}"
    
    # Process multiple tables with different outcomes
    tables_to_process = [
        {"name": "customers", "records": 50000, "success": True, "duration": 30.5},
        {"name": "orders", "records": 200000, "success": True, "duration": 45.2},
        {"name": "products", "records": 10000, "success": False, "duration": 5.1},
        {"name": "inventory", "records": 75000, "success": True, "duration": 35.8},
        {"name": "transactions", "records": 500000, "success": True, "duration": 120.3}
    ]
    
    for i, table_info in enumerate(tables_to_process):
        operation_time = batch_start_time + timedelta(minutes=i * 15)
        table_name = table_info["name"]
        record_count = table_info["records"]
        success = table_info["success"]
        duration = table_info["duration"]
        
        if success:
            logger.log_operation(
                notebook_name="BatchProcessor",
                table_name=table_name,
                operation_type="BATCH_PROCESS",
                rows_before=0,
                rows_after=record_count,
                execution_time=duration,
                message=f"Batch {batch_id} - Successfully processed {table_name}",
                custom_timestamp=operation_time
            )
        else:
            logger.log_operation(
                notebook_name="BatchProcessor",
                table_name=table_name,
                operation_type="BATCH_PROCESS",
                execution_time=duration,
                error_message=f"Schema validation failed for {table_name}",
                message=f"Batch {batch_id} - Failed to process {table_name}",
                custom_timestamp=operation_time
            )
    
    # Log batch completion summary
    batch_end_time = batch_start_time + timedelta(hours=2)
    successful_tables = sum(1 for t in tables_to_process if t["success"])
    total_tables = len(tables_to_process)
    
    logger.log_operation(
        notebook_name="BatchProcessor",
        table_name="batch_control",
        operation_type="BATCH_COMPLETE",
        execution_time=sum(t["duration"] for t in tables_to_process),
        message=f"Batch {batch_id} completed - {successful_tables}/{total_tables} tables processed successfully",
        custom_timestamp=batch_end_time
    )

comprehensive_batch_processing()

# =============================================================================
# EXAMPLE 11: Advanced Semantic Model Operations
# =============================================================================

def advanced_semantic_model_operations():
    """Example of advanced semantic model operations"""
    
    print("\nPerforming advanced semantic model operations...")
    
    # Check current model status
    logger.show_complete_status()
    
    # Enhance with measure folders and data types
    print("\nEnhancing semantic model...")
    enhancement_success = logger.enhance_semantic_model()
    
    if enhancement_success:
        print("Enhancement successful! Checking measure organization...")
        
        # Verify measure folders (this would require TOM access in practice)
        try:
            from sempy_labs.tom import connect_semantic_model
            
            with connect_semantic_model(
                dataset=logger.semantic_model_name,
                readonly=True,
                workspace=logger.workspace_name
            ) as tom_model:
                
                print("\nCurrent measure organization:")
                for table in tom_model.model.Tables:
                    if table.Name == 'monitoring_log':
                        folders = {}
                        for measure in table.Measures:
                            folder = measure.DisplayFolder or 'Root'
                            if folder not in folders:
                                folders[folder] = []
                            folders[folder].append(measure.Name)
                        
                        for folder, measures in folders.items():
                            print(f"  {folder}: {', '.join(measures)}")
                        break
                        
        except Exception as e:
            print(f"Could not verify measure organization: {e}")
    
    else:
        print("Enhancement failed. Model may need manual refresh.")

advanced_semantic_model_operations()

# =============================================================================
# EXAMPLE 12: Time Intelligence and Analytics Preparation
# =============================================================================

def prepare_time_intelligence_data():
    """Prepare data optimized for time intelligence analysis"""
    
    print("\nPreparing time intelligence data...")
    
    # Create operations across different time patterns
    base_date = datetime.now() - timedelta(days=30)
    
    # Daily operations for the past month
    for day in range(30):
        operation_date = base_date + timedelta(days=day)
        
        # Skip weekends for some operations (realistic pattern)
        if operation_date.weekday() < 5:  # Monday = 0, Sunday = 6
            
            # Morning batch job
            morning_time = operation_date.replace(hour=6, minute=0, second=0)
            logger.log_operation(
                notebook_name="DailyBatch",
                table_name="daily_metrics",
                operation_type="DAILY_BATCH",
                rows_before=50000,
                rows_after=51000 + (day * 100),  # Growing data
                execution_time=25.0 + (day * 0.5),  # Increasing time
                message=f"Daily batch for {operation_date.strftime('%Y-%m-%d')}",
                custom_timestamp=morning_time
            )
            
            # Evening summary
            evening_time = operation_date.replace(hour=18, minute=30, second=0)
            logger.log_operation(
                notebook_name="DailySummary",
                table_name="summary_reports",
                operation_type="SUMMARIZE",
                rows_before=0,
                rows_after=100,
                execution_time=5.2,
                message=f"Daily summary for {operation_date.strftime('%Y-%m-%d')}",
                custom_timestamp=evening_time
            )
    
    print(f"Created 30 days of time intelligence data")
    print("Data now supports:")
    print("  - Day-over-day analysis")
    print("  - Week-over-week trends")
    print("  - Month-to-date calculations")
    print("  - Business day vs weekend patterns")

prepare_time_intelligence_data()

# =============================================================================
# FINAL STATUS AND SUMMARY
# =============================================================================

print("\n" + "="*60)
print("ALL EXAMPLES COMPLETED")
print("="*60)
print("Framework features demonstrated:")
print("  ✓ Basic operation logging")
print("  ✓ Historical data with custom timestamps")
print("  ✓ Time-based operation patterns")
print("  ✓ Error handling and recovery scenarios")
print("  ✓ Performance monitoring")
print("  ✓ Data quality tracking")
print("  ✓ Batch processing workflows")
print("  ✓ Enhanced semantic model operations")
print("  ✓ Time intelligence data preparation")
print("\nCheck your results with:")
print("  logger.show_recent(20)")
print("  logger.get_statistics()")
print("  logger.show_complete_status()")
print("="*60)

# Show final summary
logger.show_recent(10)
final_stats = logger.get_statistics()
print(f"\nFinal Statistics: {final_stats}")
