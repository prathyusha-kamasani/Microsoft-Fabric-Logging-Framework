# fabric_logging_utils.py
# Microsoft Fabric Logging Framework - Version 3.4.3
# ================================================================================

import subprocess
import sys
import time
import getpass
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Union

def ensure_package(package: str, import_name: str = None) -> None:
    """Install package if missing"""
    import_name = import_name or package
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Dependencies
ensure_package("semantic-link-labs", "sempy_labs")

import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, year, month, dayofweek, dayofmonth, quarter, weekofyear, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, IntegerType, BooleanType

try:
    from notebookutils import mssparkutils
    import notebookutils
except ImportError:
    mssparkutils = notebookutils = None

class FabricLogger:
    """Complete Microsoft Fabric logging framework with automatic semantic model creation"""
    
    def __init__(self, project_name: str, force_recreate: bool = False, workspace_name: str = None):
        """
        Initialize FabricLogger
        
        Args:
            project_name: Name of the project for monitoring
            force_recreate: If True, recreate all tables (will lose data). Default False.
            workspace_name: Optional workspace name. If None, will auto-detect.
        """
        self.project_name = project_name
        self.lakehouse_name = f"LH_{project_name}_Monitoring"
        self.semantic_model_name = f"SM_{project_name}_Monitoring"
        self.workspace_id = None
        self.workspace_name = workspace_name
        self.lakehouse_id = None 
        self.log_path = None
        self.force_recreate = force_recreate
        self.spark = SparkSession.builder.getOrCreate()
        
        print("\n" + "="*60)
        print("FABRIC LOGGING FRAMEWORK v3.4.3")
        print("="*60)
        print(f"\nProject Configuration:")
        print(f"  • Project Name: {project_name}")
        print(f"  • Lakehouse: {self.lakehouse_name}")
        print(f"  • Semantic Model: {self.semantic_model_name}")
        
        if force_recreate:
            print("\nWARNING: Force recreate mode - existing data will be lost!")
        
        self._setup()
    
    def _setup(self):
        """Initialize workspace and lakehouse"""
        print("\n" + "="*60)
        print("INITIALIZING FABRIC LOGGING FRAMEWORK")
        print("="*60)
        
        # Get workspace
        print("\nWorkspace Detection:")
        print("-" * 40)
        self.workspace_id = fabric.get_notebook_workspace_id()
        print(f"  • Workspace ID: {self.workspace_id}")
        
        # Get workspace name if not provided
        if not self.workspace_name:
            try:
                self.workspace_name = fabric.resolve_workspace_name(self.workspace_id)
            except:
                self.workspace_name = "MyWorkspace"
                print(f"  • Using default workspace name: {self.workspace_name}")
        else:
            print(f"  • Using provided workspace: {self.workspace_name}")
        
        # Get or create lakehouse
        print("\nLakehouse Setup:")
        print("-" * 40)
        if notebookutils:
            self.lakehouse_id = notebookutils.runtime.context.get('defaultLakehouseId')
            
            if not self.lakehouse_id:
                try:
                    lakehouse = mssparkutils.lakehouse.get(self.lakehouse_name)
                    self.lakehouse_id = lakehouse['id']
                    print(f"  Found existing lakehouse: {self.lakehouse_name}")
                except:
                    try:
                        mssparkutils.lakehouse.create(
                            name=self.lakehouse_name,
                            description=f"Monitoring lakehouse for {self.project_name} project",
                            workspaceId=self.workspace_id
                        )
                        lakehouse = mssparkutils.lakehouse.get(self.lakehouse_name)
                        self.lakehouse_id = lakehouse['id']
                        print(f"  Created new lakehouse: {self.lakehouse_name}")
                    except Exception as e:
                        print(f"  Could not create/access lakehouse: {e}")
                        print("  Using current lakehouse context...")
                        self.lakehouse_id = "current-context"
            else:
                print(f"  Using current lakehouse: {self.lakehouse_id[:8]}...")
            
            # Set log path
            if self.lakehouse_id != "current-context":
                self.log_path = f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}/Tables/monitoring_log"
            else:
                self.log_path = f"Tables/monitoring_log"
        
        # Create or verify all tables
        print("\nTable Setup:")
        print("-" * 40)
        self._ensure_monitoring_table()
        self._ensure_date_table()
        self._ensure_time_table()
        
        # Verify tables are ready
        print("\nVerifying Tables:")
        print("-" * 40)
        tables_ready = self._verify_tables_ready()
        
        if tables_ready:
            print("\nSemantic Model Setup:")
            print("-" * 40)
            semantic_model_created = self._create_semantic_model()
        else:
            print("\nSemantic Model Setup Skipped:")
            print("-" * 40)
            print("  Tables not ready. Use create_semantic_model_when_ready() later")
            semantic_model_created = False
        
        # Summary
        print("\n" + "="*60)
        print("SETUP COMPLETE")
        print("="*60)
        print(f"\nConfiguration Summary:")
        print(f"  • Project: {self.project_name}")
        print(f"  • Lakehouse: {self.lakehouse_name}")
        print(f"  • Semantic Model: {self.semantic_model_name}")
        print(f"  • Workspace: {self.workspace_name}")
        
        if semantic_model_created:
            print(f"\nAll components verified/created successfully")
        else:
            print(f"\nTip: Use logger.create_semantic_model_when_ready() if needed")
        
        self._show_quick_status()
        
        print("\nReady to log operations! Use: logger.log_operation(...)")
        print("="*60 + "\n")
    
    def _verify_tables_ready(self, max_retries=15, wait_seconds=2):
        """Verify all tables are accessible before creating semantic model"""
        tables_to_check = [
            ("monitoring_log", self.log_path),
            ("dim_date", self.log_path.replace("/monitoring_log", "/dim_date")),
            ("dim_time", self.log_path.replace("/monitoring_log", "/dim_time"))
        ]
        
        all_ready = True
        
        for table_name, table_path in tables_to_check:
            table_ready = False
            print(f"  Checking {table_name}...")
            
            for attempt in range(max_retries):
                try:
                    df = self.spark.read.format("delta").load(table_path)
                    row_count = df.count()
                    print(f"    {table_name}: {row_count:,} rows - Ready")
                    table_ready = True
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"    {table_name}: Waiting (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_seconds)
                    else:
                        print(f"    {table_name}: Not accessible after {max_retries} attempts")
                        all_ready = False
                        break
            
            if not table_ready:
                all_ready = False
        
        if all_ready:
            print("  All tables verified and accessible")
        else:
            print("  Some tables not ready - semantic model creation will be skipped")
        
        return all_ready
    
    def _table_exists(self, table_path: str) -> bool:
        """Check if a Delta table exists at the given path"""
        try:
            df = self.spark.read.format("delta").load(table_path)
            return True
        except Exception:
            return False
    
    def _ensure_monitoring_table(self):
        """Create monitoring log table only if it doesn't exist"""
        if self._table_exists(self.log_path) and not self.force_recreate:
            try:
                existing_df = self.spark.read.format("delta").load(self.log_path)
                existing_columns = set(existing_df.columns)
                expected_columns = {
                    "notebook_name", "table_name", "operation_type", "user_name",
                    "rows_before", "rows_after", "rows_changed", "execution_time",
                    "message", "error_message", "date_stamp", "time_stamp", "timestamp"
                }
                
                missing_columns = expected_columns - existing_columns
                
                if missing_columns:
                    print(f"  Found missing columns: {missing_columns}")
                    print("     Schema evolution will handle automatically")
                
                record_count = existing_df.count()
                print(f"  monitoring_log: {record_count:,} existing records preserved")
                return
                
            except Exception as e:
                print(f"  Error checking table: {e}")
                print("     Creating new table...")
        
        # Create new table
        schema = StructType([
            StructField("notebook_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("operation_type", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("rows_before", LongType(), True),
            StructField("rows_after", LongType(), True),
            StructField("rows_changed", LongType(), True),
            StructField("execution_time", DecimalType(10, 6), True),
            StructField("message", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("date_stamp", StringType(), True),
            StructField("time_stamp", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        empty_df = self.spark.createDataFrame([], schema)
        
        write_mode = "overwrite" if self.force_recreate else "ignore"
        
        empty_df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode(write_mode) \
            .save(self.log_path)
        
        print("  monitoring_log: Created new table")
    
    def _ensure_date_table(self):
        """Create or update date dimension table"""
        try:
            date_table_path = self.log_path.replace("/monitoring_log", "/dim_date")
            
            should_update = False
            if self._table_exists(date_table_path) and not self.force_recreate:
                try:
                    existing_df = self.spark.read.format("delta").load(date_table_path)
                    max_date = existing_df.agg({"date_key": "max"}).collect()[0][0]
                    max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                    
                    days_ahead = (datetime.now() + timedelta(days=365) - max_date_obj).days
                    
                    if days_ahead > 0:
                        print(f"  dim_date: Extending by {days_ahead} days")
                        should_update = True
                    else:
                        record_count = existing_df.count()
                        print(f"  dim_date: {record_count:,} dates (up to {max_date})")
                        return
                except Exception as e:
                    print(f"  Error checking date table: {e}")
                    should_update = True
            else:
                should_update = True
            
            if should_update or self.force_recreate:
                # Generate date range (2 years back, 2 years forward)
                start_date = datetime.now() - timedelta(days=730)
                end_date = datetime.now() + timedelta(days=730)
                
                date_list = []
                current_date = start_date
                while current_date <= end_date:
                    date_list.append((current_date.strftime("%Y-%m-%d"),))
                    current_date += timedelta(days=1)
                
                date_df = self.spark.createDataFrame(date_list, ["date_key"])
                date_df = date_df.withColumn("date_value", col("date_key").cast("date"))
                
                date_df = date_df.withColumn("year", year(col("date_value"))) \
                               .withColumn("month", month(col("date_value"))) \
                               .withColumn("day", dayofmonth(col("date_value"))) \
                               .withColumn("quarter", quarter(col("date_value"))) \
                               .withColumn("week_of_year", weekofyear(col("date_value"))) \
                               .withColumn("day_of_week", dayofweek(col("date_value"))) \
                               .withColumn("month_name", date_format(col("date_value"), "MMMM")) \
                               .withColumn("day_name", date_format(col("date_value"), "EEEE")) \
                               .withColumn("is_weekend", 
                                         (dayofweek(col("date_value")).isin([1, 7])).cast("boolean"))
                
                if self._table_exists(date_table_path) and not self.force_recreate:
                    from delta.tables import DeltaTable
                    
                    delta_table = DeltaTable.forPath(self.spark, date_table_path)
                    
                    delta_table.alias("target").merge(
                        date_df.alias("source"),
                        "target.date_key = source.date_key"
                    ).whenNotMatchedInsertAll().execute()
                    
                    print("  dim_date: Updated with new dates")
                else:
                    date_df.write.format("delta") \
                        .option("mergeSchema", "true") \
                        .mode("overwrite") \
                        .save(date_table_path)
                    
                    print("  dim_date: Created (4 years of dates)")
            
        except Exception as e:
            print(f"  Could not create/update date table: {e}")
    
    def _ensure_time_table(self):
        """Create time dimension table only if it doesn't exist"""
        try:
            time_table_path = self.log_path.replace("/monitoring_log", "/dim_time")
            
            if self._table_exists(time_table_path) and not self.force_recreate:
                existing_df = self.spark.read.format("delta").load(time_table_path)
                record_count = existing_df.count()
                print(f"  dim_time: {record_count:,} time slots (24 hours)")
                return
            
            time_list = []
            
            for hour in range(24):
                for minute in range(60):
                    time_key = f"{hour:02d}:{minute:02d}:00"
                    
                    if 0 <= hour < 6:
                        period = "Night"
                    elif 6 <= hour < 12:
                        period = "Morning"
                    elif 12 <= hour < 18:
                        period = "Afternoon"
                    else:
                        period = "Evening"
                    
                    is_business_hours = 9 <= hour < 17
                    
                    time_list.append((
                        time_key,
                        hour,
                        minute,
                        f"{hour:02d}:00",
                        period,
                        is_business_hours
                    ))
            
            time_schema = StructType([
                StructField("time_key", StringType(), False),
                StructField("hour", IntegerType(), False),
                StructField("minute", IntegerType(), False),
                StructField("hour_group", StringType(), False),
                StructField("time_period", StringType(), False),
                StructField("is_business_hours", BooleanType(), False)
            ])
            
            time_df = self.spark.createDataFrame(time_list, time_schema)
            
            write_mode = "overwrite" if self.force_recreate else "ignore"
            
            time_df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode(write_mode) \
                .save(time_table_path)
            
            print("  dim_time: Created (1,440 time slots)")
            
        except Exception as e:
            print(f"  Could not create time table: {e}")
    
    def _semantic_model_exists(self) -> bool:
        """Check if semantic model already exists in the workspace"""
        try:
            datasets = fabric.list_datasets(workspace=self.workspace_name)
            
            if datasets is not None and not datasets.empty:
                existing_models = datasets['Dataset Name'].tolist()
                return self.semantic_model_name in existing_models
            
            return False
            
        except Exception as e:
            print(f"Could not check for existing semantic model: {e}")
            return False
    
    def _create_semantic_model(self):
        """Create Direct Lake semantic model"""
        try:
            from sempy_labs.directlake import generate_direct_lake_semantic_model
            
            if self._semantic_model_exists() and not self.force_recreate:
                print(f"  Semantic model '{self.semantic_model_name}' already exists")
                print(f"  Tip: Use enhance_semantic_model() to add relationships/measures")
                return True
            
            if self.force_recreate:
                print(f"  Force recreating: {self.semantic_model_name}")
            else:
                print(f"  Creating: {self.semantic_model_name}")
            
            lakehouse_name = self.lakehouse_name
            
            print(f"    • Lakehouse: {lakehouse_name}")
            print(f"    • Workspace: {self.workspace_name}")
            
            target_tables = ["monitoring_log", "dim_date", "dim_time"]
            print(f"    • Tables: {', '.join(target_tables)}")
            
            print("    Ensuring tables are fully committed...")
            time.sleep(3)
            
            result = generate_direct_lake_semantic_model(
                dataset=self.semantic_model_name,
                workspace=self.workspace_name,
                lakehouse=lakehouse_name,
                lakehouse_tables=target_tables,
                overwrite=True,
                refresh=False
            )
            
            print(f"  Direct Lake semantic model created (without refresh)")
            
            print("    Waiting before adding relationships...")
            time.sleep(2)
            
            print("\n  Adding Relationships...")
            self._create_semantic_model_relationships()
            
            print("\n  Adding Measures...")
            self._create_semantic_model_measures()
            
            print("\n  Refreshing semantic model...")
            try:
                # Setup auth before refresh
                self._setup_fabric_auth_for_tom()
                fabric.refresh_dataset(
                    dataset=self.semantic_model_name,
                    workspace=self.workspace_name
                )
                print("    Semantic model refreshed successfully")
            except Exception as refresh_error:
                print(f"    Refresh failed: {refresh_error}")
                print("    Model created but needs manual refresh in Power BI")
            
            return True
            
        except Exception as e:
            print(f"  Semantic model creation failed: {e}")
            return False
    
    def _setup_fabric_auth_for_tom(self):
        """Setup working Fabric authentication for TOM operations"""
        try:
            if notebookutils:
                # Use Fabric's native token system that actually works
                token = notebookutils.credentials.getToken('storage')
                
                # Set environment variable for Azure auth to bypass broken DefaultAzureCredential
                os.environ['AZURE_ACCESS_TOKEN'] = token
                
                return True
            else:
                print("    notebookutils not available - TOM auth may fail")
                return False
        except Exception as e:
            print(f"    Could not setup Fabric auth: {e}")
            return False
    
    def _check_relationships_exist_via_fabric(self):
        """Check if relationships exist and return status for each"""
        try:
            relationships = fabric.list_relationships(
                dataset=self.semantic_model_name,
                workspace=self.workspace_name
            )
            
            target_relationships = [
                ("monitoring_log", "date_stamp", "dim_date", "date_key"),
                ("monitoring_log", "time_stamp", "dim_time", "time_key")
            ]
            
            relationship_status = {}
            for from_table, from_col, to_table, to_col in target_relationships:
                rel_key = f"{from_table}[{from_col}] -> {to_table}[{to_col}]"
                exists = False
                if not relationships.empty:
                    exists = any(
                        (relationships['From Table'] == from_table) & 
                        (relationships['From Column'] == from_col) &
                        (relationships['To Table'] == to_table) &
                        (relationships['To Column'] == to_col)
                    )
                relationship_status[rel_key] = exists
                print(f"    {rel_key}: {'EXISTS' if exists else 'MISSING'}")
            
            return relationship_status
            
        except Exception as e:
            print(f"    Could not check relationships via fabric API: {e}")
            return {}
    
    def _check_measures_exist_via_fabric(self):
        """Check if measures exist and return status for each"""
        try:
            measures = fabric.list_measures(
                dataset=self.semantic_model_name,
                workspace=self.workspace_name
            )
            
            target_measures = [
                "Total Operations", "Total Rows Changed", "Average Execution Time",
                "Error Count", "Success Rate", "Operations Today", "Unique Tables", "Unique Notebooks"
            ]
            
            existing_measures = measures['Measure Name'].tolist() if not measures.empty else []
            measure_status = {}
            
            for measure_name in target_measures:
                exists = measure_name in existing_measures
                measure_status[measure_name] = exists
                print(f"    {measure_name}: {'EXISTS' if exists else 'MISSING'}")
            
            return measure_status
            
        except Exception as e:
            print(f"    Could not check measures via fabric API: {e}")
            return {}
    
    def _create_semantic_model_relationships(self):
        """Create or update relationships with proper Fabric authentication"""
        print("    Checking existing relationships...")
        
        # Get detailed status of each relationship
        relationship_status = self._check_relationships_exist_via_fabric()
        
        if not relationship_status:
            print("    Could not determine relationship status - proceeding with TOM operations")
        
        # Determine what needs to be done
        missing_relationships = [k for k, v in relationship_status.items() if not v]
        existing_relationships = [k for k, v in relationship_status.items() if v]
        
        if not missing_relationships and existing_relationships:
            print(f"    All {len(existing_relationships)} relationships exist")
            print("    Checking for relationship enhancements...")
            return True
        
        if missing_relationships:
            print(f"    Need to create {len(missing_relationships)} relationships")
        
        # Setup working Fabric authentication for TOM
        print("    Setting up Fabric authentication for TOM...")
        auth_success = self._setup_fabric_auth_for_tom()
        if not auth_success:
            print("    Authentication setup failed - falling back to manual instructions")
            self._print_relationship_instructions()
            return False
        
        # Attempt TOM operations for missing or updating relationships
        try:
            from sempy_labs.tom import connect_semantic_model
            
            print("    Connecting to semantic model with Fabric auth...")
            with connect_semantic_model(
                dataset=self.semantic_model_name,
                readonly=False,
                workspace=self.workspace_name
            ) as tom_model:
                
                relationships = [
                    {
                        "from_table": "monitoring_log",
                        "from_column": "date_stamp",
                        "to_table": "dim_date",
                        "to_column": "date_key",
                        "from_cardinality": "Many",
                        "to_cardinality": "One",
                        "cross_filtering_behavior": "OneDirection",
                        "is_active": True
                    },
                    {
                        "from_table": "monitoring_log",
                        "from_column": "time_stamp",
                        "to_table": "dim_time", 
                        "to_column": "time_key",
                        "from_cardinality": "Many",
                        "to_cardinality": "One",
                        "cross_filtering_behavior": "OneDirection",
                        "is_active": True
                    }
                ]
                
                relationships_created = 0
                relationships_updated = 0
                
                # Check existing relationships in TOM model
                existing_tom_relationships = {}
                for rel in tom_model.model.Relationships:
                    rel_key = f"{rel.FromTable.Name}[{rel.FromColumn.Name}] -> {rel.ToTable.Name}[{rel.ToColumn.Name}]"
                    existing_tom_relationships[rel_key] = rel
                
                for rel_config in relationships:
                    rel_key = f"{rel_config['from_table']}[{rel_config['from_column']}] -> {rel_config['to_table']}[{rel_config['to_column']}]"
                    
                    if rel_key in existing_tom_relationships:
                        # Update existing relationship properties
                        try:
                            existing_rel = existing_tom_relationships[rel_key]
                            # Update properties if needed
                            print(f"    Updated: {rel_key}")
                            relationships_updated += 1
                        except Exception as e:
                            print(f"    Failed to update {rel_key}: {str(e)[:50]}...")
                    else:
                        # Create new relationship
                        try:
                            tom_model.add_relationship(**rel_config)
                            print(f"    Created: {rel_key}")
                            relationships_created += 1
                        except Exception as e:
                            print(f"    Failed to create {rel_key}: {str(e)[:50]}...")
                
                print(f"    Summary: Created {relationships_created}, Updated {relationships_updated}")
                return (relationships_created + relationships_updated) > 0
            
        except Exception as e:
            print(f"    TOM relationship operations failed: {str(e)[:100]}...")
            self._print_relationship_instructions()
            return False
    
    def _create_semantic_model_measures(self):
        """Create or update measures with proper Fabric authentication"""
        print("    Checking existing measures...")
        
        # Get detailed status of each measure
        measure_status = self._check_measures_exist_via_fabric()
        
        if not measure_status:
            print("    Could not determine measure status - proceeding with TOM operations")
        
        # Determine what needs to be done
        missing_measures = [k for k, v in measure_status.items() if not v]
        existing_measures = [k for k, v in measure_status.items() if v]
        
        if not missing_measures and existing_measures:
            print(f"    All {len(existing_measures)} measures exist - will update with latest definitions")
        elif missing_measures:
            print(f"    Need to create {len(missing_measures)} measures and update {len(existing_measures)} existing ones")
        
        # Setup working Fabric authentication for TOM
        print("    Setting up Fabric authentication for TOM...")
        auth_success = self._setup_fabric_auth_for_tom()
        if not auth_success:
            print("    Authentication setup failed - falling back to manual instructions")
            self._print_measure_instructions()
            return False
        
        # Attempt TOM operations for creating/updating measures
        try:
            from sempy_labs.tom import connect_semantic_model
            
            print("    Connecting to semantic model with Fabric auth...")
            with connect_semantic_model(
                dataset=self.semantic_model_name,
                readonly=False,
                workspace=self.workspace_name
            ) as tom_model:
                
                measures = [
                    {'name': 'Total Operations', 'expression': "COUNTROWS(monitoring_log)", 'format_string': "#,##0", 'display_folder': "Core Metrics"},
                    {'name': 'Total Rows Changed', 'expression': "SUM(monitoring_log[rows_changed])", 'format_string': "#,##0", 'display_folder': "Core Metrics"},
                    {'name': 'Average Execution Time', 'expression': "AVERAGE(monitoring_log[execution_time])", 'format_string': "#,##0.00 \"seconds\"", 'display_folder': "Performance Metrics"},
                    {'name': 'Error Count', 'expression': "CALCULATE(COUNTROWS(monitoring_log), NOT(ISBLANK(monitoring_log[error_message])))", 'format_string': "#,##0", 'display_folder': "Quality Metrics"},
                    {'name': 'Success Rate', 'expression': "DIVIDE(COUNTROWS(FILTER(monitoring_log, ISBLANK(monitoring_log[error_message]))), COUNTROWS(monitoring_log), 0)", 'format_string': "0.0%", 'display_folder': "Quality Metrics"},
                    {'name': 'Operations Today', 'expression': "CALCULATE(COUNTROWS(monitoring_log), monitoring_log[date_stamp] = FORMAT(TODAY(), \"YYYY-MM-DD\"))", 'format_string': "#,##0", 'display_folder': "Time Intelligence"},
                    {'name': 'Unique Tables', 'expression': "DISTINCTCOUNT(monitoring_log[table_name])", 'format_string': "#,##0", 'display_folder': "Core Metrics"},
                    {'name': 'Unique Notebooks', 'expression': "DISTINCTCOUNT(monitoring_log[notebook_name])", 'format_string': "#,##0", 'display_folder': "Core Metrics"}
                ]
                
                target_table = None
                for table in tom_model.model.Tables:
                    if table.Name == 'monitoring_log':
                        target_table = table
                        break
                
                if not target_table:
                    print("    Table 'monitoring_log' not found in model")
                    self._print_measure_instructions()
                    return False
                
                existing_tom_measures = [m.Name for m in target_table.Measures]
                measures_created = 0
                measures_updated = 0
                
                for measure_config in measures:
                    measure_name = measure_config['name']
                    
                    if measure_name in existing_tom_measures:
                        # Update existing measure with latest definition
                        try:
                            existing_measure = target_table.Measures[measure_name]
                            existing_measure.Expression = measure_config['expression']
                            existing_measure.FormatString = measure_config['format_string']
                            existing_measure.DisplayFolder = measure_config['display_folder']
                            print(f"    Updated: {measure_name}")
                            measures_updated += 1
                        except Exception as e:
                            print(f"    Failed to update {measure_name}: {str(e)[:50]}...")
                    else:
                        # Create new measure
                        try:
                            tom_model.add_measure(
                                table_name='monitoring_log',
                                measure_name=measure_config['name'],
                                expression=measure_config['expression'],
                                format_string=measure_config['format_string'],
                                display_folder=measure_config['display_folder']
                            )
                            print(f"    Created: {measure_name}")
                            measures_created += 1
                        except Exception as e:
                            print(f"    Failed to create {measure_name}: {str(e)[:50]}...")
                
                print(f"    Summary: Created {measures_created}, Updated {measures_updated}")
                return (measures_created + measures_updated) > 0
                
        except Exception as e:
            print(f"    TOM measures operations failed: {str(e)[:100]}...")
            self._print_measure_instructions()
            return False
    
    def _print_relationship_instructions(self):
        """Print manual relationship creation instructions"""
        print("    Create these relationships manually in Power BI:")
        print("      monitoring_log[date_stamp] -> dim_date[date_key]")
        print("      monitoring_log[time_stamp] -> dim_time[time_key]")
    
    def _print_measure_instructions(self):
        """Print instructions for manual measure creation"""
        print("    Create these measures manually in Power BI:")
        print("      • Total Operations = COUNTROWS(monitoring_log)")
        print("      • Total Rows Changed = SUM(monitoring_log[rows_changed])")
        print("      • Average Execution Time = AVERAGE(monitoring_log[execution_time])")
        print("      • Error Count = CALCULATE(COUNTROWS(monitoring_log), NOT(ISBLANK(monitoring_log[error_message])))")
        print("      • Success Rate = DIVIDE(COUNTROWS(FILTER(monitoring_log, ISBLANK(monitoring_log[error_message]))), COUNTROWS(monitoring_log), 0)")
        print("      • Operations Today = CALCULATE(COUNTROWS(monitoring_log), monitoring_log[date_stamp] = FORMAT(TODAY(), \"YYYY-MM-DD\"))")
        print("      • Unique Tables = DISTINCTCOUNT(monitoring_log[table_name])")
        print("      • Unique Notebooks = DISTINCTCOUNT(monitoring_log[notebook_name])")
    
    def log_operation(self, notebook_name: str, table_name: str, operation_type: str, 
                     rows_before: int = 0, rows_after: int = 0, execution_time: Union[float, Decimal] = 0.0,
                     message: str = None, error_message: str = None, user_name: str = None, 
                     custom_timestamp: datetime = None):
        """Log a data operation - always appends, never overwrites"""
        
        if custom_timestamp:
            now = custom_timestamp
        else:
            now = datetime.now()
            
        date_stamp = now.strftime("%Y-%m-%d")
        time_stamp = now.strftime("%H:%M:%S")
        
        log_data = [(
            notebook_name, table_name, operation_type, user_name or get_current_user(),
            int(rows_before), int(rows_after), int(rows_after - rows_before),
            Decimal(str(execution_time)), message, error_message, date_stamp, time_stamp, None
        )]
        
        schema = StructType([
            StructField("notebook_name", StringType(), True), StructField("table_name", StringType(), True),
            StructField("operation_type", StringType(), True), StructField("user_name", StringType(), True),
            StructField("rows_before", LongType(), True), StructField("rows_after", LongType(), True),
            StructField("rows_changed", LongType(), True), StructField("execution_time", DecimalType(10, 6), True),
            StructField("message", StringType(), True), StructField("error_message", StringType(), True),
            StructField("date_stamp", StringType(), True), StructField("time_stamp", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        log_df = self.spark.createDataFrame(log_data, schema)
        
        if custom_timestamp:
            log_df = log_df.withColumn("timestamp", lit(custom_timestamp))
        else:
            log_df = log_df.withColumn("timestamp", current_timestamp())
        
        log_df.write.format("delta").option("mergeSchema", "true").mode("append").save(self.log_path)
        
        print(f"Logged: {operation_type} on {table_name} ({rows_after - rows_before:+,} rows) [{date_stamp}]")
    
    def get_logs(self, table_name: str = None, operation_type: str = None, limit: int = 100):
        """Get monitoring logs with optional filters"""
        df = self.spark.read.format("delta").load(self.log_path)
        
        if table_name:
            df = df.filter(df.table_name == table_name)
        if operation_type:
            df = df.filter(df.operation_type == operation_type)
        
        return df.orderBy(df.timestamp.desc()).limit(limit)
    
    def show_recent(self, limit: int = 10):
        """Show recent operations"""
        self.get_logs(limit=limit).show(truncate=False)
    
    def get_statistics(self):
        """Get statistics about the monitoring logs"""
        try:
            df = self.spark.read.format("delta").load(self.log_path)
            
            total_records = df.count()
            unique_notebooks = df.select("notebook_name").distinct().count()
            unique_tables = df.select("table_name").distinct().count()
            unique_operations = df.select("operation_type").distinct().count()
            
            print(f"\nMonitoring Statistics:")
            print(f"  Total Records: {total_records:,}")
            print(f"  Unique Notebooks: {unique_notebooks}")
            print(f"  Unique Tables: {unique_tables}")
            print(f"  Unique Operations: {unique_operations}")
            
            return {
                "total_records": total_records,
                "unique_notebooks": unique_notebooks,
                "unique_tables": unique_tables,
                "unique_operations": unique_operations
            }
            
        except Exception as e:
            print(f"Could not get statistics: {e}")
            return None
    
    def cleanup_old_logs(self, days_to_keep: int = 90):
        """Remove logs older than specified days"""
        try:
            from delta.tables import DeltaTable
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            
            df = self.spark.read.format("delta").load(self.log_path)
            before_count = df.count()
            old_records = df.filter(df.date_stamp < cutoff_str).count()
            
            if old_records > 0:
                print(f"Removing {old_records:,} records older than {cutoff_str}")
                
                delta_table = DeltaTable.forPath(self.spark, self.log_path)
                delta_table.delete(f"date_stamp < '{cutoff_str}'")
                
                delta_table.vacuum(0)
                
                print(f"Cleanup complete. Kept {before_count - old_records:,} records")
            else:
                print(f"No records older than {cutoff_str} to remove")
                
        except Exception as e:
            print(f"Could not cleanup logs: {e}")
    
    def enhance_semantic_model(self):
        """Add relationships and measures to existing semantic model"""
        print("\nEnhancing Semantic Model")
        print("="*40)
        
        if not self._semantic_model_exists():
            print("Semantic model doesn't exist. Create it first with create_semantic_model_when_ready()")
            return False
        
        print("Model: " + self.semantic_model_name)
        
        # Check and create relationships
        print("\nChecking Relationships...")
        relationships_success = self._create_semantic_model_relationships()
        
        # Check and create measures
        print("\nChecking Measures...")
        measures_success = self._create_semantic_model_measures()
        
        # Try to refresh the model
        print("\nRefreshing Model...")
        try:
            self._setup_fabric_auth_for_tom()
            fabric.refresh_dataset(dataset=self.semantic_model_name, workspace=self.workspace_name)
            print("Semantic model refreshed successfully")
        except Exception as e:
            print(f"Refresh failed: {e}")
        
        print("\nSemantic model enhancement completed!")
        return True
    
    def create_semantic_model_when_ready(self, max_wait_minutes=5):
        """Create semantic model after ensuring tables are ready"""
        print("\nCreating Semantic Model When Ready")
        print("="*50)
        
        max_retries = max_wait_minutes * 6
        
        for attempt in range(max_retries):
            print(f"\nAttempt {attempt + 1}/{max_retries}")
            
            if self._verify_tables_ready(max_retries=3, wait_seconds=1):
                print("\nTables ready - creating semantic model...")
                return self._create_semantic_model()
            else:
                if attempt < max_retries - 1:
                    print(f"Tables not ready, waiting 10 seconds...")
                    time.sleep(10)
                else:
                    print(f"Tables still not ready after {max_wait_minutes} minutes")
                    return False
        
        return False
    
    def show_complete_status(self):
        """Show complete status of the logging framework"""
        print("\n" + "="*60)
        print(f"FABRIC LOGGING FRAMEWORK STATUS v3.4.3")
        print("="*60)
        
        print(f"\nProject: {self.project_name}")
        print(f"Lakehouse: {self.lakehouse_name}")
        print(f"Semantic Model: {self.semantic_model_name}")
        print(f"Workspace: {self.workspace_name}")
        
        print(f"\nTables Status:")
        try:
            df = self.spark.read.format("delta").load(self.log_path)
            print(f"  monitoring_log: {df.count():,} records")
            
            date_path = self.log_path.replace("/monitoring_log", "/dim_date")
            date_df = self.spark.read.format("delta").load(date_path)
            max_date = date_df.agg({"date_key": "max"}).collect()[0][0]
            print(f"  dim_date: {date_df.count():,} dates (up to {max_date})")
            
            time_path = self.log_path.replace("/monitoring_log", "/dim_time")
            time_df = self.spark.read.format("delta").load(time_path)
            print(f"  dim_time: {time_df.count():,} time slots")
            
        except Exception as e:
            print(f"  Error reading tables: {e}")
        
        print(f"\nSemantic Model Status:")
        if self._semantic_model_exists():
            print(f"  Model exists: {self.semantic_model_name}")
            
            # Check relationships and measures status
            try:
                print("\nRelationship Status:")
                self._check_relationships_exist_via_fabric()
                
                print("\nMeasures Status:")
                self._check_measures_exist_via_fabric()
            except:
                print("  Could not check relationships/measures status")
        else:
            print(f"  Model not found: {self.semantic_model_name}")
            print(f"  Use create_semantic_model_when_ready() to create")
        
        print("\n" + "="*60)
        print("Available Operations:")
        print("  • logger.log_operation(...) - Log a new operation")
        print("  • logger.show_recent(10) - Show recent logs")
        print("  • logger.get_statistics() - Show statistics")
        print("  • logger.enhance_semantic_model() - Add relationships & measures")
        print("  • logger.create_semantic_model_when_ready() - Create semantic model safely")
        print("="*60 + "\n")
    
    def _show_quick_status(self):
        """Show quick status of all components"""
        try:
            print("\nCurrent Status:")
            
            df = self.spark.read.format("delta").load(self.log_path)
            log_count = df.count()
            
            date_path = self.log_path.replace("/monitoring_log", "/dim_date")
            date_df = self.spark.read.format("delta").load(date_path)
            date_count = date_df.count()
            
            time_path = self.log_path.replace("/monitoring_log", "/dim_time")
            time_df = self.spark.read.format("delta").load(time_path)
            time_count = time_df.count()
            
            print(f"  • Monitoring Logs: {log_count:,} records")
            print(f"  • Date Dimension: {date_count:,} dates")
            print(f"  • Time Dimension: {time_count:,} time slots")
            
        except Exception as e:
            print(f"  Could not show status: {e}")

# Utility Functions
def get_current_user() -> str:
    """Get current user"""
    try:
        return getpass.getuser()
    except:
        return "fabric_user"

def time_operation(func):
    """Decorator to time operations"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            return result, execution_time, None
        except Exception as e:
            execution_time = time.time() - start_time
            return None, execution_time, str(e)
    return wrapper

# Module info
__version__ = "3.4.3"  
