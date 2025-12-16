#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MYSQLMIGRATOR - MySQL Database Migration Tool

A powerful command-line tool for MySQL database migration, written in Python.
Migrate schema, data, indexes, triggers, and stored procedures between MySQL databases.

Author: @RIKIPB of WeblyArts | www.weblyarts.com
License: MIT License
Copyright (c) 2025 @RIKIPB of WeblyArts | www.weblyarts.com
"""

import os
import json
import sys
import re
from typing import Dict, List, Optional, Tuple, Union
import getpass

# We'll use pymysql for MySQL connection
try:
    import pymysql
except ImportError:
    print("PyMySQL is required. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymysql"])
    import pymysql

# ASCII Art for MYSQLMIGRATOR
MYSQLMIGRATOR_ASCII = """
///////////////////////////////////////////////////////////
//  __  ____   ______  ___  _     __  __ _                _             //
// |  \/  \ \ / / ___|| _ \| |   |  \/  (_)__ _ _ _ __ _| |_ ___ _ _   //
// | |\/| |\ V /\___ \|  _/| |__ | |\/| | / _` | '_/ _` |  _/ _ \ '_|  //
// |_|  |_| |_| |___/_|_|  |____||_|  |_|_\__, |_| \__,_|\__\___/_|    //
//                                        |___/                         //
///////////////////////////////////////////////////////////
"""

CONFIG_FILE = ".migrator-env"

class MySQLMigrator:
    def __init__(self):
        self.master_config = {
            "host": "",
            "user": "",
            "password": "",
            "database": ""
        }
        self.destination_configs = []
        self.config_loaded = False
        
    def load_config(self) -> bool:
        """Load configuration from .migrator-env file if exists"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.master_config = config.get('master_config', {})
                    self.destination_configs = config.get('destination_configs', [])
                    self.config_loaded = True
                    return True
            except Exception as e:
                print(f"Error loading configuration: {e}")
                return False
        return False
    
    def save_config(self) -> bool:
        """Save configuration to .migrator-env file"""
        try:
            with open(CONFIG_FILE, 'w') as f:
                config = {
                    'master_config': self.master_config,
                    'destination_configs': self.destination_configs
                }
                json.dump(config, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving configuration: {e}")
            return False
    
    def reset_config(self) -> bool:
        """Delete the configuration file"""
        if os.path.exists(CONFIG_FILE):
            try:
                os.remove(CONFIG_FILE)
                self.master_config = {
                    "host": "",
                    "user": "",
                    "password": "",
                    "database": ""
                }
                self.destination_configs = []
                self.config_loaded = False
                return True
            except Exception as e:
                print(f"Error resetting configuration: {e}")
                return False
        return True
    
    def setup_wizard(self) -> bool:
        """Run the setup wizard to configure databases"""
        print("\nMySQL Database Configuration Wizard")
        print("================================")
        
        # Ask for master database configuration
        print("\nMaster Database Configuration:")
        self.master_config["host"] = input("Enter host (default: localhost): ").strip() or "localhost"
        self.master_config["user"] = input("Enter username: ").strip()
        if not self.master_config["user"]:
            print("Username cannot be empty.")
            return False
        
        self.master_config["password"] = getpass.getpass("Enter password: ")
        self.master_config["database"] = input("Enter database name: ").strip()
        if not self.master_config["database"]:
            print("Database name cannot be empty.")
            return False
        
        # Test connection to master
        try:
            conn = pymysql.connect(
                host=self.master_config["host"],
                user=self.master_config["user"],
                password=self.master_config["password"],
                database=self.master_config["database"]
            )
            conn.close()
            print("Successfully connected to master database.")
        except Exception as e:
            print(f"Failed to connect to master database: {e}")
            return False
        
        # Ask for destination databases
        self.destination_configs = []
        print("\nDestination Database Configuration:")
        print("You can add multiple destination databases.")
        
        while True:
            dest_config = {
                "host": input("\nEnter destination host (or leave empty to finish): ").strip()
            }
            
            if not dest_config["host"]:
                break
                
            dest_config["user"] = input("Enter destination username: ").strip()
            dest_config["password"] = getpass.getpass("Enter destination password: ")
            dest_config["database"] = input("Enter destination database name: ").strip()
            
            # Test connection to destination
            try:
                conn = pymysql.connect(
                    host=dest_config["host"],
                    user=dest_config["user"],
                    password=dest_config["password"],
                    database=dest_config["database"]
                )
                conn.close()
                print("Successfully connected to destination database.")
                self.destination_configs.append(dest_config)
            except Exception as e:
                print(f"Failed to connect to destination database: {e}")
                continue
        
        if not self.destination_configs:
            print("At least one destination database is required.")
            return False
        
        # Ask to save configuration
        save_config = input("\nSave this configuration for future use? (y/n): ").lower()
        if save_config == 'y':
            return self.save_config()
        
        return True
    
    def get_connection(self, config):
        """Get a MySQL connection based on config"""
        return pymysql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4'
        )
    
    def get_table_schema(self, config: Dict) -> Dict[str, Dict]:
        """Get the schema of all tables in a database"""
        schema = {}
        try:
            conn = self.get_connection(config)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            
            for table_row in tables:
                table_name = list(table_row.values())[0]  # Get the table name from the dict
                
                # Get table schema
                cursor.execute(f"DESCRIBE `{table_name}`;")
                columns = cursor.fetchall()
                schema[table_name] = columns
                
                # Get create table statement
                cursor.execute(f"SHOW CREATE TABLE `{table_name}`;")
                create_table = cursor.fetchone()
                schema[f"{table_name}_create"] = create_table["Create Table"]
                
                # Get table collation
                cursor.execute(f"SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{config['database']}' AND TABLE_NAME = '{table_name}';")
                collation_result = cursor.fetchone()
                if collation_result:
                    schema[f"{table_name}_collation"] = collation_result['TABLE_COLLATION']
                
            conn.close()
            return schema
        except Exception as e:
            print(f"Error getting schema from {config['database']}: {e}")
            return {}
            
    def get_indexes(self, config: Dict) -> Dict[str, List]:
        """Get all indexes from a database"""
        indexes = {}
        try:
            conn = self.get_connection(config)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            
            for table_row in tables:
                table_name = list(table_row.values())[0]  # Get the table name from the dict
                
                # Get indexes for this table
                cursor.execute(f"SHOW INDEX FROM `{table_name}`;")
                table_indexes = cursor.fetchall()
                
                if table_indexes:
                    indexes[table_name] = table_indexes
            
            conn.close()
            return indexes
        except Exception as e:
            print(f"Error getting indexes from {config['database']}: {e}")
            return {}
    
    def get_triggers(self, config: Dict) -> Dict[str, Dict]:
        """Get all triggers from a database"""
        triggers = {}
        try:
            conn = self.get_connection(config)
            cursor = conn.cursor()
            
            # Get all triggers
            cursor.execute("SHOW TRIGGERS;")
            all_triggers = cursor.fetchall()
            
            for trigger in all_triggers:
                trigger_name = trigger['Trigger']
                
                # Get create trigger statement
                cursor.execute(f"SHOW CREATE TRIGGER `{trigger_name}`;")
                create_trigger = cursor.fetchone()
                
                if create_trigger:
                    triggers[trigger_name] = {
                        'info': trigger,
                        'create_statement': create_trigger['SQL Original Statement']
                    }
            
            conn.close()
            return triggers
        except Exception as e:
            print(f"Error getting triggers from {config['database']}: {e}")
            return {}
    
    def get_procedures(self, config: Dict) -> Dict[str, Dict]:
        """Get all stored procedures from a database"""
        procedures = {}
        try:
            conn = self.get_connection(config)
            cursor = conn.cursor()
            
            # Get all procedures
            cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = '{config['database']}';")
            all_procedures = cursor.fetchall()
            
            for proc in all_procedures:
                proc_name = proc['Name']
                
                # Get create procedure statement
                cursor.execute(f"SHOW CREATE PROCEDURE `{proc_name}`;")
                create_proc = cursor.fetchone()
                
                if create_proc:
                    procedures[proc_name] = {
                        'info': proc,
                        'create_statement': create_proc['Create Procedure']
                    }
            
            conn.close()
            return procedures
        except Exception as e:
            print(f"Error getting procedures from {config['database']}: {e}")
            return {}
    
    def migrate_indexes(self, dest_config: Dict) -> bool:
        """Migrate indexes from master database to destination database"""
        try:
            # Get indexes from master
            master_indexes = self.get_indexes(self.master_config)
            if not master_indexes:
                print("No indexes found in master database or error retrieving indexes")
                return True  # Not a critical error
            
            # Connect to destination
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Get existing tables in destination
            dest_cursor.execute("SHOW TABLES;")
            dest_tables = [list(table.values())[0] for table in dest_cursor.fetchall()]
            
            # Get destination database collation
            dest_collation = self.get_database_collation(dest_config)
            
            for table_name, indexes in master_indexes.items():
                if table_name not in dest_tables:
                    print(f"Table {table_name} does not exist in destination, skipping indexes")
                    continue
                
                # Get column information to check for text columns
                dest_cursor.execute(f"SHOW FULL COLUMNS FROM `{table_name}`;")
                columns_info = {col["Field"]: col for col in dest_cursor.fetchall()}
                
                # Drop existing indexes (except primary key)
                dest_cursor.execute(f"SHOW INDEXES FROM `{table_name}` WHERE Key_name != 'PRIMARY';")
                existing_indexes = dest_cursor.fetchall()
                existing_index_names = set()
                
                for idx in existing_indexes:
                    idx_name = idx["Key_name"]
                    if idx_name not in existing_index_names:
                        dest_cursor.execute(f"DROP INDEX `{idx_name}` ON `{table_name}`;")
                        existing_index_names.add(idx_name)
                
                # Create indexes from master
                for index_name, index_info in indexes.items():
                    if index_name == "PRIMARY":
                        continue  # Skip primary keys as they're part of table schema
                    
                    columns = index_info["columns"]
                    is_unique = index_info["unique"]
                    
                    # Check if any column is a text type with utf8mb4 collation
                    modified_columns = []
                    for col in columns:
                        if col in columns_info:
                            col_type = columns_info[col]["Type"].lower()
                            collation = columns_info[col]["Collation"]
                            
                            # Check if it's a text column with utf8mb4 collation
                            if collation and "utf8mb4" in collation and any(text_type in col_type for text_type in ["char", "text", "enum", "set"]):
                                # Limit index length for utf8mb4 text columns
                                if "varchar" in col_type:
                                    # Extract size from varchar(X)
                                    match = re.search(r'varchar\((\d+)\)', col_type)
                                    if match:
                                        size = int(match.group(1))
                                        # If size * 4 > 1000 (max key length), limit it
                                        if size * 4 > 1000:
                                            max_chars = min(size, 191)  # 191 * 4 = 764 bytes
                                            modified_columns.append(f"`{col}`({max_chars})")
                                            continue
                                else:  # For text, longtext, etc.
                                    modified_columns.append(f"`{col}`(191)")
                                    continue
                            
                        modified_columns.append(f"`{col}`")
                    
                    columns_str = ", ".join(modified_columns)
                    unique_str = "UNIQUE" if is_unique else ""
                    
                    try:
                        create_index_stmt = f"CREATE {unique_str} INDEX `{index_name}` ON `{table_name}` ({columns_str});"
                        dest_cursor.execute(create_index_stmt)
                        print(f"Created index {index_name} on {table_name}")
                    except Exception as idx_e:
                        print(f"Error creating index {index_name}: {idx_e}")
                        # If error contains key length issue, try with more aggressive length limitation
                        if "max key length" in str(idx_e).lower():
                            print("Attempting with shorter key length...")
                            modified_columns = []
                            for col in columns:
                                if col in columns_info:
                                    col_type = columns_info[col]["Type"].lower()
                                    if any(text_type in col_type for text_type in ["char", "text", "enum", "set"]):
                                        modified_columns.append(f"`{col}`(100)")
                                    else:
                                        modified_columns.append(f"`{col}`")
                                else:
                                    modified_columns.append(f"`{col}`")
                            
                            columns_str = ", ".join(modified_columns)
                            create_index_stmt = f"CREATE {unique_str} INDEX `{index_name}` ON `{table_name}` ({columns_str});"
                            try:
                                dest_cursor.execute(create_index_stmt)
                                print(f"Created index {index_name} with reduced key length")
                            except Exception as retry_e:
                                print(f"Failed to create index even with reduced length: {retry_e}")
            
            dest_conn.commit()
            dest_conn.close()
            return True
        except Exception as e:
            print(f"Error migrating indexes to {dest_config['database']}: {e}")
            return False
    
    def migrate_triggers(self, dest_config: Dict) -> bool:
        """Migrate triggers from master to destination database"""
        try:
            # Get triggers from master
            master_triggers = self.get_triggers(self.master_config)
            if not master_triggers:
                print("No triggers found in master database or error retrieving triggers.")
                return True  # Not a failure if no triggers exist
            
            # Connect to destination database
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Get existing triggers in destination
            dest_cursor.execute("SHOW TRIGGERS;")
            dest_triggers = {trigger['Trigger']: trigger for trigger in dest_cursor.fetchall()}
            
            # Process each trigger
            for trigger_name, trigger_data in master_triggers.items():
                # Check if trigger exists in destination
                if trigger_name in dest_triggers:
                    # Drop existing trigger
                    dest_cursor.execute(f"DROP TRIGGER IF EXISTS `{trigger_name}`;")
                
                # Create trigger
                create_stmt = trigger_data['create_statement']
                dest_cursor.execute(create_stmt)
                print(f"Created trigger {trigger_name} in {dest_config['database']}")
            
            dest_conn.commit()
            dest_conn.close()
            return True
        except Exception as e:
            print(f"Error migrating triggers to {dest_config['database']}: {e}")
            return False
    
    def migrate_procedures(self, dest_config: Dict) -> bool:
        """Migrate stored procedures from master to destination database"""
        try:
            # Get procedures from master
            master_procedures = self.get_procedures(self.master_config)
            if not master_procedures:
                print("No stored procedures found in master database or error retrieving procedures.")
                return True  # Not a failure if no procedures exist
            
            # Connect to destination database
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Get existing procedures in destination
            dest_cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = '{dest_config['database']}';")
            dest_procedures = {proc['Name']: proc for proc in dest_cursor.fetchall()}
            
            # Process each procedure
            for proc_name, proc_data in master_procedures.items():
                # Check if procedure exists in destination
                if proc_name in dest_procedures:
                    # Drop existing procedure
                    dest_cursor.execute(f"DROP PROCEDURE IF EXISTS `{proc_name}`;")
                
                # Create procedure
                create_stmt = proc_data['create_statement']
                dest_cursor.execute(create_stmt)
                print(f"Created procedure {proc_name} in {dest_config['database']}")
            
            dest_conn.commit()
            dest_conn.close()
            return True
        except Exception as e:
            print(f"Error migrating procedures to {dest_config['database']}: {e}")
            return False
            
    def get_database_collation(self, config: Dict) -> str:
        """Get the default collation of the database"""
        try:
            conn = self.get_connection(config)
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{config['database']}';")
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result['DEFAULT_COLLATION_NAME']
            return 'utf8mb4_unicode_ci'  # Default fallback
        except Exception as e:
            print(f"Error getting database collation: {e}")
            return 'utf8mb4_unicode_ci'  # Default fallback
    
    def standardize_collation(self, create_stmt: str, target_collation: str) -> str:
        """Aggressively standardize and fix collations in CREATE TABLE statement"""
        
        # 1. Force default charset to utf8mb4
        create_stmt = re.sub(r'DEFAULT\s+CHARSET=\w+', 'DEFAULT CHARSET=utf8mb4', create_stmt, flags=re.IGNORECASE)
        
        # 2. Force table-level collation
        create_stmt = re.sub(r'COLLATE\s*[\w_]+', f'COLLATE {target_collation}', create_stmt, flags=re.IGNORECASE)
        
        # 3. Fix column-level: any CHARACTER SET ... COLLATE ...
        create_stmt = re.sub(
            r'CHARACTER\s+SET\s+\w+\s+COLLATE\s+[\w_]+',
            f'CHARACTER SET utf8mb4 COLLATE {target_collation}',
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # 4. Fix cases where only COLLATE is specified (e.g., COLLATE latin1_general_ci)
        create_stmt = re.sub(
            r'\bCOLLATE\s+[\w_]+',
            f'COLLATE {target_collation}',
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # 5. Remove any remaining references to non-utf8mb4 collations (nuclear option if needed)
        # This removes any lingering "latin1_general_ci", "latin1_swedish_ci", etc.
        create_stmt = re.sub(
            r'\b(latin1|utf8|ucs2|utf16|utf32)[_\w]*ci\b',
            target_collation,
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # 6. Ensure any remaining CHARACTER SET without COLLATE gets utf8mb4 + correct collation
        create_stmt = re.sub(
            r'CHARACTER\s+SET\s+\w+',
            f'CHARACTER SET utf8mb4 COLLATE {target_collation}',
            create_stmt,
            flags=re.IGNORECASE
        )
        
        return create_stmt
            
    def overwrite_schema(self, dest_config: Dict) -> bool:
        """Overwrite the schema in destination database with master schema"""
        try:
            master_schema = self.get_table_schema(self.master_config)
            if not master_schema:
                print(f"Could not retrieve schema from master database: {self.master_config['database']}")
                return False
            
            # Get destination database collation
            dest_collation = self.get_database_collation(dest_config)
            print(f"Destination database collation: {dest_collation}")
            
            # Connect to destination database
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Get existing tables in destination
            dest_cursor.execute("SHOW TABLES;")
            dest_tables = [list(table.values())[0] for table in dest_cursor.fetchall()]
            
            # Drop existing tables
            for table in dest_tables:
                dest_cursor.execute(f"DROP TABLE IF EXISTS `{table}`;")
            
            # Create tables from master schema
            for table_name in master_schema:
                if table_name.endswith("_create"):  # This is a create table statement
                    original_table_name = table_name[:-7]  # Remove "_create" suffix
                    create_stmt = master_schema[table_name]
                    
                    # Standardize collation
                    create_stmt = self.standardize_collation(create_stmt, dest_collation)
                    
                    try:
                        dest_cursor.execute(create_stmt)
                        print(f"Created table {original_table_name} in {dest_config['database']}")
                    except Exception as e:
                        print(f"Error creating table {original_table_name}: {e}")
                        print("Attempting to create with default utf8mb4_unicode_ci collation...")
                        create_stmt = self.standardize_collation(create_stmt, 'utf8mb4_unicode_ci')
                        dest_cursor.execute(create_stmt)
                        print(f"Created table {original_table_name} with utf8mb4_unicode_ci collation")
            
            dest_conn.commit()
            dest_conn.close()
            
            # Migrate indexes, triggers and procedures
            print("\nMigrating indexes...")
            self.migrate_indexes(dest_config)
            
            print("\nMigrating triggers...")
            self.migrate_triggers(dest_config)
            
            print("\nMigrating stored procedures...")
            self.migrate_procedures(dest_config)
            
            return True
        except Exception as e:
            print(f"Error overwriting schema in {dest_config['database']}: {e}")
            return False
    
    def update_schema(self, dest_config: Dict) -> bool:
        """Update the schema in destination database with master schema (add missing fields only)"""
        try:
            master_schema = self.get_table_schema(self.master_config)
            dest_schema = self.get_table_schema(dest_config)
            
            if not master_schema:
                print(f"Could not retrieve schema from master database: {self.master_config['database']}")
                return False
            
            # Get destination database collation
            dest_collation = self.get_database_collation(dest_config)
            print(f"Destination database collation: {dest_collation}")
            
            # Connect to destination database
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Process each table in master schema
            for table_name in master_schema:
                if table_name.endswith("_create") or table_name.endswith("_collation"):
                    continue  # Skip create statements and collation info for now
                
                if table_name in dest_schema:
                    # Table exists, check for missing columns
                    master_columns = {col["Field"]: col for col in master_schema[table_name]}
                    dest_columns = {col["Field"]: col for col in dest_schema[table_name]}
                    
                    for col_name, col_info in master_columns.items():
                        if col_name not in dest_columns:
                            # Add missing column
                            col_type = col_info["Type"]
                            null_str = "NULL" if col_info["Null"] == "YES" else "NOT NULL"
                            default_str = f"DEFAULT '{col_info['Default']}'" if col_info['Default'] is not None else ""
                            
                            # Add collation for text columns
                            collation_str = ""
                            if any(text_type in col_type.lower() for text_type in ["char", "text", "enum", "set"]):
                                collation_str = f" CHARACTER SET utf8mb4 COLLATE {dest_collation}"
                            
                            alter_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}{collation_str} {null_str} {default_str};"
                            try:
                                dest_cursor.execute(alter_stmt)
                                print(f"Added column {col_name} to table {table_name} in {dest_config['database']}")
                            except Exception as col_e:
                                print(f"Error adding column with specified collation: {col_e}")
                                print("Attempting with utf8mb4_unicode_ci collation...")
                                if collation_str:
                                    collation_str = " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                                    alter_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}{collation_str} {null_str} {default_str};"
                                    dest_cursor.execute(alter_stmt)
                                    print(f"Added column {col_name} with utf8mb4_unicode_ci collation")
                else:
                    # Table doesn't exist, create it
                    create_stmt = master_schema[f"{table_name}_create"]
                    
                    # Standardize collation
                    create_stmt = self.standardize_collation(create_stmt, dest_collation)
                    
                    try:
                        dest_cursor.execute(create_stmt)
                        print(f"Created table {table_name} in {dest_config['database']}")
                    except Exception as e:
                        print(f"Error creating table {table_name}: {e}")
                        print("Attempting to create with default utf8mb4_unicode_ci collation...")
                        create_stmt = self.standardize_collation(create_stmt, 'utf8mb4_unicode_ci')
                        dest_cursor.execute(create_stmt)
                        print(f"Created table {table_name} with utf8mb4_unicode_ci collation")
            
            dest_conn.commit()
            dest_conn.close()
            
            # Migrate indexes, triggers and procedures
            print("\nMigrating indexes...")
            self.migrate_indexes(dest_config)
            
            print("\nMigrating triggers...")
            self.migrate_triggers(dest_config)
            
            print("\nMigrating stored procedures...")
            self.migrate_procedures(dest_config)
            
            return True
        except Exception as e:
            print(f"Error updating schema in {dest_config['database']}: {e}")
            return False
    
    def migrate_data(self, dest_config: Dict, where_clause: str = "") -> bool:
        """Migrate data from master to destination database"""
        try:
            master_schema = self.get_table_schema(self.master_config)
            dest_schema = self.get_table_schema(dest_config)
            
            master_conn = self.get_connection(self.master_config)
            master_cursor = master_conn.cursor()
            
            dest_conn = self.get_connection(dest_config)
            dest_cursor = dest_conn.cursor()
            
            # Process each table in master schema
            for table_name in master_schema:
                if table_name.endswith("_create"):
                    continue  # Skip create statements
                    
                if table_name not in dest_schema:
                    print(f"Table {table_name} does not exist in destination database. Skipping data migration.")
                    continue
                
                # Get column names that exist in both databases
                master_columns = {col["Field"]: col for col in master_schema[table_name]}
                dest_columns = {col["Field"]: col for col in dest_schema[table_name]}
                
                common_columns = [col for col in master_columns if col in dest_columns]
                if not common_columns:
                    print(f"No common columns found for table {table_name}. Skipping.")
                    continue
                
                columns_str = ", ".join([f"`{col}`" for col in common_columns])
                
                # Construct query with optional WHERE clause
                query = f"SELECT {columns_str} FROM `{table_name}`"
                if where_clause:
                    query += f" WHERE {where_clause}"
                
                master_cursor.execute(query)
                rows = master_cursor.fetchall()
                
                if rows:
                    # Clear existing data in destination table
                    dest_cursor.execute(f"DELETE FROM `{table_name}`")
                    
                    # Insert data in batches
                    batch_size = 1000
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        
                        # Prepare the placeholders and values for the insert
                        placeholders = ", ".join(["%s"] * len(common_columns))
                        insert_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
                        
                        # Extract values from each row
                        values = []
                        for row in batch:
                            row_values = [row[col] for col in common_columns]
                            values.append(row_values)
                        
                        # Execute the batch insert
                        dest_cursor.executemany(insert_query, values)
                        
                    print(f"Migrated {len(rows)} rows to table {table_name} in {dest_config['database']}")
            
            dest_conn.commit()
            master_conn.close()
            dest_conn.close()
            return True
        except Exception as e:
            print(f"Error migrating data to {dest_config['database']}: {e}")
            return False
    
    def run(self):
        """Main execution method"""
        print(MYSQLMIGRATOR_ASCII)
        print("\nWelcome to MYSQLMIGRATOR!")
        
        # Check for existing configuration
        if self.load_config():
            print(f"Configuration loaded from {CONFIG_FILE}")
            print(f"Master DB: {self.master_config['database']} on {self.master_config['host']}")
            print("Destination DBs:")
            for i, dest in enumerate(self.destination_configs, 1):
                print(f"  {i}. {dest['database']} on {dest['host']}")
        else:
            print("No configuration found.")
            if not self.setup_wizard():
                print("Setup failed. Exiting.")
                return
        
        while True:
            print("\nMYSQLMIGRATOR Options:")
            print("1. Overwrite DB Schema (replace destination schema with master schema)")
            print("2. Update DB Schema (add missing fields only)")
            print("3. Migrate Data")
            print("4. Reset Configuration")
            print("5. Exit")
            
            choice = input("\nEnter your choice (1-5): ")
            
            if choice == '1':
                for dest_config in self.destination_configs:
                    print(f"\nOverwriting schema in {dest_config['database']} on {dest_config['host']}...")
                    if self.overwrite_schema(dest_config):
                        print(f"Schema overwrite successful for {dest_config['database']}")
                    else:
                        print(f"Schema overwrite failed for {dest_config['database']}")
            
            elif choice == '2':
                for dest_config in self.destination_configs:
                    print(f"\nUpdating schema in {dest_config['database']} on {dest_config['host']}...")
                    if self.update_schema(dest_config):
                        print(f"Schema update successful for {dest_config['database']}")
                    else:
                        print(f"Schema update failed for {dest_config['database']}")
            
            elif choice == '3':
                where_clause = input("\nEnter optional WHERE clause for data migration (leave empty for all data): ")
                for dest_config in self.destination_configs:
                    print(f"\nMigrating data to {dest_config['database']} on {dest_config['host']}...")
                    if self.migrate_data(dest_config, where_clause):
                        print(f"Data migration successful for {dest_config['database']}")
                    else:
                        print(f"Data migration failed for {dest_config['database']}")
            
            elif choice == '4':
                confirm = input("Are you sure you want to reset configuration? (y/n): ").lower()
                if confirm == 'y':
                    if self.reset_config():
                        print("Configuration reset successful.")
                        if not self.setup_wizard():
                            print("Setup failed. Exiting.")
                            return
                    else:
                        print("Configuration reset failed.")
            
            elif choice == '5':
                print("Thank you for using MYSQLMIGRATOR. Goodbye!")
                break
            
            else:
                print("Invalid choice. Please try again.")

if __name__ == "__main__":
    migrator = MySQLMigrator()
    migrator.run()
