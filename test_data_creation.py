# test_data_creation.py  
# Generate realistic test data for the Fabric Logging Framework
# Run this after setting up your logger to populate it with sample data

from fabric_logging_utils import FabricLogger
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
        
        # Create the log entry (but don't use the actual timestamp - let the logger set it)
        logger.log_operation(
            notebook_name=notebook,
            table_name=table,
            operation_type=operation,
            rows_before=rows_before,
            rows_after=rows_after,
            execution_time=execution_time,
            message=success_msg,
            error_message=error_msg,
            user_name=random.choice(["alice.smith", "bob.jones", "carol.davis", "david.wilson", "eve.brown"])
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

def generate_time_series_data(logger, days
