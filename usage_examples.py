# test_data_creation.py  
# Generate realistic test data for the Fabric Logging Framework
# Run this after setting up your logger to populate it with sample data

import builtin.fabric_logging_utils as fabric_logging_utils
from builtin.fabric_logging_utils import FabricLogger, time_operation
import random
from datetime import datetime, timedelta

def create_test_data(logger, num_operations=50):
    """
    Generate realistic test data for the logging framework
    
    Args:
        logger: FabricLogger instance
        num_operations: Number of test operations to create
    """
    
    print(f"Creating {num_operations} test operations...")
    
    # Sample data for realistic test scenarios
    notebooks = [
        "DataIngestion", "ETL_Pipeline", "DataValidation", "DataCleaning",
        "Analytics_Prep", "ML_Feature_Engineering", "Reporting_ETL",
        "Data_Quality_Check", "Incremental_Load", "Batch_Processor"
    ]
    
    tables = [
        "customers", "orders", "products", "sales_fact", "inventory",
        "transactions", "user_activity", "marketing_campaigns", 
        "financial_data", "operational_metrics", "customer_segments",
        "product_catalog", "order_items", "shipping_data", "returns"
    ]
    
    operations = [
        "INSERT", "UPDATE", "DELETE", "MERGE", "LOAD", "EXTRACT",
        "TRANSFORM", "VALIDATE", "CLEANSE", "AGGREGATE", "JOIN",
        "DEDUPLICATE", "QUALITY_CHECK", "REFRESH", "BACKUP"
    ]
    
    success_messages = [
        "Operation completed successfully",
        "Data loaded from source system", 
        "Transformation rules applied",
        "Quality validation passed",
        "Incremental update completed",
        "Batch processing finished",
        "Data refresh successful",
        "Merge operation completed",
        "Validation rules applied",
        "Cleansing process completed"
    ]
    
    error_messages = [
        "Connection timeout to source system",
        "Data validation failed - null values found",
        "Duplicate key constraint violation", 
        "Insufficient permissions for table access",
        "Memory allocation exceeded during join",
        "Invalid data format in source file",
        "Schema mismatch between source and target",
        "Network interruption during data transfer",
        "Concurrent modification detected",
        "File not found in expected location"
    ]
    
    # Generate test operations with realistic patterns
    base_time = datetime.now() - timedelta(days=30)  # Start 30 days ago
    
    for i in range(num_operations):
        # Create realistic timestamp progression
        operation_time = base_time + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(6, 22),  # Business hours mostly
            minutes=random.randint(0, 59)
        )
        
        # Choose random but realistic values
        notebook = random.choice(notebooks)
        table = random.choice(tables)
        operation = random.choice(operations)
        
        # Generate realistic row counts based on table type
        if table in ["customers", "products", "user_activity"]:
            base_rows = random.randint(10000, 100000)
        elif table in ["orders", "transactions", "sales_fact"]:
            base_rows = random.randint(50000, 500000)
        else:
            base_rows = random.randint(1000, 50000)
        
        # Calculate rows_before and rows_after based on operation type
        if operation in ["INSERT", "LOAD", "EXTRACT"]:
            rows_before = random.randint(0, base_rows // 2)
            rows_after = rows_before + random.randint(1000, base_rows)
        elif operation in ["UPDATE", "MERGE", "TRANSFORM"]:
            rows_before = base_rows
            rows_after = rows_before + random.randint(-1000, 1000)
        elif operation in ["DELETE", "CLEANSE"]:
            rows_before = base_rows
            rows_after = rows_before - random.randint(10, base_rows // 10)
        else:  # Validation, quality checks, etc.
            rows_before = base_rows
            rows_after = rows_before
        
        # Make sure rows_after is not negative
        rows_after = max(0, rows_after)
        
        # Generate realistic execution times based on operation and data size
        data_factor = (rows_after + rows_before) / 100000  # Scale based on data size
        base_time_factor = {
            "INSERT": 2.0, "UPDATE": 3.0, "DELETE": 1.5, "MERGE": 5.0,
            "LOAD": 4.0, "EXTRACT": 3.0, "TRANSFORM": 6.0, "VALIDATE": 1.0,
            "CLEANSE": 4.0, "AGGREGATE": 3.0, "JOIN": 8.0, "DEDUPLICATE": 5.0,
            "QUALITY_CHECK": 2.0, "REFRESH": 1.0, "BACKUP": 10.0
        }.get(operation, 2.0)
        
        execution_time = round(base_time_factor * data_factor * random.uniform(0.5, 2.0), 2)
        
        # 90% success rate, 10% errors
        has_error = random.random() < 0.1
        
        if has_error:
            error_msg = random.choice(error_messages)
            success_msg = None
            # Errors might have different row counts
            if operation in ["INSERT", "LOAD"]:
                rows_after = rows_before + random.randint(0, (rows_after - rows_before) // 2)
        else:
            error_msg = None
            success_msg = random.choice(success_messages)
        
        # Create the log entry with custom timestamp for historical data
        logger.log_operation(
            notebook_name=notebook,
            table_name=table,
            operation_type=operation,
            rows_before=rows_before,
            rows_after=rows_after,
            execution_time=execution_time,
            message=success_msg,
            error_message=error_msg,
            user_name=random.choice(["alice.smith", "bob.jones", "carol.davis", "david.wilson", "eve.brown"]),
            custom_timestamp=operation_time  # Use the generated historical timestamp
        )
        
        # Show progress every 10 operations
        if (i + 1) % 10 == 0:
            print(f"  Created {i + 1}/{num_operations} operations...")
    
    print(f"✅ Created {num_operations} test operations successfully!")
    return num_operations

def create_realistic_scenarios(logger):
    """Create specific realistic scenarios for testing"""
    
    print("\nCreating realistic test scenarios...")
    
    # Scenario 1: Daily ETL Pipeline
    print("  Scenario 1: Daily ETL Pipeline")
    pipeline_operations = [
        ("DataIngestion", "source_customers", "EXTRACT", 0, 50000, 45.2),
        ("ETL_Pipeline", "staging_customers", "TRANSFORM", 50000, 49500, 120.7),
        ("ETL_Pipeline", "dim_customer", "LOAD", 45000, 49500, 85.3),
        ("DataValidation", "dim_customer", "VALIDATE", 49500, 49500, 12.1)
    ]
    
    for notebook, table, operation, before, after, time in pipeline_operations:
        logger.log_operation(
            notebook_name=notebook,
            table_name=table,
            operation_type=operation,
            rows_before=before,
            rows_after=after,
            execution_time=time,
            message=f"Daily ETL pipeline step: {operation}"
        )
    
    # Scenario 2: Data Quality Issues
    print("  Scenario 2: Data Quality Issues")
    logger.log_operation(
        notebook_name="DataQuality",
        table_name="orders",
        operation_type="QUALITY_CHECK",
        rows_before=100000,
        rows_after=98500,
        execution_time=25.4,
        error_message="1500 records failed validation - missing customer_id",
        message="Quality check found data issues"
    )
    
    # Scenario 3: Performance Issues
    print("  Scenario 3: Performance Issues")
    logger.log_operation(
        notebook_name="Analytics_Prep",
        table_name="sales_fact",
        operation_type="AGGREGATE",
        rows_before=5000000,
        rows_after=50000,
        execution_time=450.8,
        message="Monthly aggregation - performance was slower than usual"
    )
    
    # Scenario 4: Incremental Load
    print("  Scenario 4: Incremental Load")
    logger.log_operation(
        notebook_name="Incremental_Load",
        table_name="transactions",
        operation_type="MERGE",
        rows_before=2000000,
        rows_after=2005000,
        execution_time=67.2,
        message="Incremental load: 5000 new transactions merged"
    )
    
    # Scenario 5: System Recovery
    print("  Scenario 5: System Recovery")
    logger.log_operation(
        notebook_name="BatchProcessor",
        table_name="product_catalog",
        operation_type="BACKUP",
        rows_before=25000,
        rows_after=25000,
        execution_time=180.5,
        message="Nightly backup completed successfully"
    )
    
    print("✅ Realistic scenarios created!")

def generate_time_series_data(logger, days=7):
    """Generate time series data showing daily patterns"""
    
    print(f"\nGenerating time series data for {days} days...")
    
    # Common operations that happen daily
    daily_operations = [
        ("DailyETL", "customers", "REFRESH", 50000, 51000, 30.0),
        ("DailyETL", "orders", "INCREMENTAL_LOAD", 200000, 205000, 45.0),
        ("DailyETL", "products", "UPDATE", 25000, 25000, 15.0),
        ("Analytics", "sales_fact", "AGGREGATE", 1000000, 1005000, 120.0),
        ("Reporting", "dashboard_data", "REFRESH", 100000, 100000, 60.0)
    ]
    
    base_date = datetime.now() - timedelta(days=days)
    
    for day in range(days):
        current_date = base_date + timedelta(days=day)
        
        # Skip weekends for some operations (realistic pattern)
        is_weekend = current_date.weekday() >= 5
        
        for notebook, table, operation, before, after, base_time in daily_operations:
            # Some operations don't run on weekends
            if is_weekend and operation in ["INCREMENTAL_LOAD", "AGGREGATE"]:
                continue
            
            # Add some variation to the execution times
            execution_time = base_time * random.uniform(0.8, 1.3)
            
            # Add some variation to row counts
            variation = random.uniform(0.95, 1.05)
            rows_before = int(before * variation)
            rows_after = int(after * variation)
            
            logger.log_operation(
                notebook_name=notebook,
                table_name=table,
                operation_type=operation,
                rows_before=rows_before,
                rows_after=rows_after,
                execution_time=execution_time,
                message=f"Daily {operation.lower()} for {current_date.strftime('%Y-%m-%d')}"
            )
    
    print(f"✅ Generated {days} days of time series data!")

def create_error_patterns(logger):
    """Create realistic error patterns for testing"""
    
    print("\nCreating error patterns...")
    
    # Common error scenarios
    error_scenarios = [
        {
            "notebook": "DataIngestion",
            "table": "external_api_data", 
            "operation": "EXTRACT",
            "error": "API rate limit exceeded - 429 status code",
            "execution_time": 5.0
        },
        {
            "notebook": "ETL_Pipeline",
            "table": "sales_staging",
            "operation": "TRANSFORM", 
            "error": "Column 'price' contains non-numeric values",
            "execution_time": 15.3
        },
        {
            "notebook": "DataValidation",
            "table": "customer_data",
            "operation": "VALIDATE",
            "error": "Primary key constraint violation - duplicate IDs found",
            "execution_time": 8.7
        },
        {
            "notebook": "ML_Pipeline",
            "table": "feature_store",
            "operation": "LOAD",
            "error": "Out of memory during feature calculation",
            "execution_time": 45.2
        },
        {
            "notebook": "Reporting",
            "table": "dashboard_cache",
            "operation": "REFRESH",
            "error": "Timeout connecting to source database",
            "execution_time": 120.0
        }
    ]
    
    for scenario in error_scenarios:
        logger.log_operation(
            notebook_name=scenario["notebook"],
            table_name=scenario["table"],
            operation_type=scenario["operation"],
            execution_time=scenario["execution_time"],
            error_message=scenario["error"],
            message="Operation failed - see error details"
        )
    
    print(f"✅ Created {len(error_scenarios)} error scenarios!")

def create_performance_benchmarks(logger):
    """Create performance benchmark data for testing"""
    
    print("\nCreating performance benchmarks...")
    
    # Different table sizes and their expected performance
    performance_tests = [
        ("Small Dataset", "test_small", 1000, 2.1),
        ("Medium Dataset", "test_medium", 50000, 15.7), 
        ("Large Dataset", "test_large", 1000000, 120.3),
        ("Very Large Dataset", "test_xlarge", 10000000, 450.8)
    ]
    
    test_operations = ["SELECT", "JOIN", "AGGREGATE", "SORT", "MERGE"]
    
    for dataset_name, table_name, record_count, base_time in performance_tests:
        for operation in test_operations:
            # Calculate realistic execution time based on operation complexity
            complexity_factor = {
                "SELECT": 1.0,
                "JOIN": 3.0, 
                "AGGREGATE": 2.0,
                "SORT": 2.5,
                "MERGE": 4.0
            }.get(operation, 1.0)
            
            execution_time = base_time * complexity_factor * random.uniform(0.8, 1.2)
            
            logger.log_operation(
                notebook_name="PerformanceTesting",
                table_name=table_name,
                operation_type=operation,
                rows_before=record_count,
                rows_after=record_count,
                execution_time=execution_time,
                message=f"Performance test: {operation} on {dataset_name} ({record_count:,} records)"
            )
    
    print("✅ Performance benchmarks created!")

# =============================================================================
# MAIN TEST DATA CREATION FUNCTION
# =============================================================================

def setup_complete_test_environment(project_name="TestProject"):
    """Set up a complete test environment with realistic data"""
    
    print("="*60)
    print("FABRIC LOGGING FRAMEWORK - TEST DATA SETUP")
    print("="*60)
    
    # Initialize the logger with custom project name
    logger = FabricLogger(project_name)
    
    print("\nGenerating comprehensive test data...")
    
    # 1. Create basic test operations
    create_test_data(logger, num_operations=30)
    
    # 2. Create realistic scenarios
    create_realistic_scenarios(logger)
    
    # 3. Generate time series data
    generate_time_series_data(logger, days=14)
    
    # 4. Create error patterns
    create_error_patterns(logger)
    
    # 5. Create performance benchmarks
    create_performance_benchmarks(logger)
    
    print("\n" + "="*60)
    print("TEST DATA CREATION COMPLETE")
    print("="*60)
    
    # Show final statistics
    stats = logger.get_statistics()
    print(f"\nFinal Statistics:")
    print(f"  Total Operations: {stats['total_records']:,}")
    print(f"  Unique Notebooks: {stats['unique_notebooks']}")
    print(f"  Unique Tables: {stats['unique_tables']}")
    print(f"  Unique Operations: {stats['unique_operations']}")
    
    print(f"\nYou can now:")
    print(f"  • View recent logs: logger.show_recent(20)")
    print(f"  • Check statistics: logger.get_statistics()")
    print(f"  • Enhance semantic model: logger.enhance_semantic_model()")
    print(f"  • View complete status: logger.show_complete_status()")
    
    return logger

# =============================================================================
# QUICK TEST FUNCTIONS
# =============================================================================

def quick_test(project_name="QuickTest"):
    """Quick test with minimal data for immediate verification"""
    
    print("Running quick test...")
    
    logger = FabricLogger(project_name)
    
    # Create just a few test operations
    test_operations = [
        ("TestNotebook", "customers", "INSERT", 0, 1000, 5.2, "Test insert operation"),
        ("TestNotebook", "orders", "UPDATE", 5000, 5100, 8.7, "Test update operation"),
        ("TestNotebook", "products", "VALIDATE", 2000, 2000, 3.1, None),
        ("TestNotebook", "sales", "MERGE", 10000, 10500, 12.4, "Test merge operation"),
        ("TestNotebook", "inventory", "DELETE", 500, 450, 2.8, None)
    ]
    
    for notebook, table, operation, before, after, time, message in test_operations:
        logger.log_operation(
            notebook_name=notebook,
            table_name=table,
            operation_type=operation,
            rows_before=before,
            rows_after=after,
            execution_time=time,
            message=message
        )
    
    print("✅ Quick test completed!")
    logger.show_recent(10)
    
    return logger

def performance_test_only():
    """Run only performance-focused tests"""
    
    logger = FabricLogger("PerformanceTest") 
    create_performance_benchmarks(logger)
    
    print("\nPerformance test completed!")
    logger.show_recent(15)
    
    return logger

def error_test_only():
    """Run only error scenario tests"""
    
    logger = FabricLogger("ErrorTest")
    create_error_patterns(logger)
    
    print("\nError test completed!")
    logger.show_recent(10)
    
    return logger

# =============================================================================
# USAGE INSTRUCTIONS
# =============================================================================

if __name__ == "__main__":
    print("""
    Fabric Logging Framework - Test Data Creation
    
    Available functions:
    
    1. setup_complete_test_environment()
       - Creates comprehensive test data with all scenarios
       - Best for full framework testing
    
    2. quick_test("ProjectName")  
       - Creates minimal test data for quick verification
       - Best for initial setup testing
    
    3. performance_test_only()
       - Creates only performance benchmark data
       - Best for performance analysis testing
    
    4. error_test_only()
       - Creates only error scenario data  
       - Best for error handling testing
    
    5. create_test_data(logger, num_operations)
       - Create custom number of random operations
       - Use with existing logger instance
    
    Example usage:
        # Quick test
        logger = quick_test("MyProject")
        
        # Full test environment  
        logger = setup_complete_test_environment()
        
        # Custom test
        logger = FabricLogger("CustomTest")
        create_test_data(logger, 100)
    """)
    
    # Uncomment one of these to run automatically:
    # logger = quick_test()
    # logger = setup_complete_test_environment()
