# MYSQLMIGRATOR

A powerful command-line tool for MySQL database migration, written in Python.

## Features

- **Complete MySQL Migration**: Migrate schema, data, indexes, triggers, and stored procedures
- **Multiple Migration Modes**:
  - **Overwrite Schema**: Replace destination schema completely with master schema
  - **Update Schema**: Add missing fields and tables without data loss
  - **Data Migration**: Copy data with optional WHERE clause filtering
- **Configuration Management**:
  - Interactive configuration wizard
  - Save/load configuration from `.migrator-env` file
  - Reset configuration option
- **Multiple Destinations**: Configure one master database and multiple destination databases
- **Collation Handling**: Automatically manages collation differences between databases
- **Index Length Management**: Handles utf8mb4 index length limitations automatically

## Requirements

- Python 3.6+
- PyMySQL library (automatically installed if missing)

## Installation

1. Download the `mysqlmigrator.py` file
2. Ensure you have Python 3.6+ installed
3. Run the script (it will automatically install required dependencies)

```bash
python mysqlmigrator.py
```

## Usage

### First Run - Configuration Wizard

On first run, the tool will guide you through setting up your master and destination database configurations:

```
MySQL Database Configuration Wizard
============================
Master Database Configuration:
Enter host (default: localhost): 
Enter user: root
Enter password: 
Enter database name: master_db

Destination Database Configuration:
Enter host (default: localhost): 
Enter user: root
Enter password: 
Enter database name: dest_db

Add another destination? (y/n): n

Save this configuration for future use? (y/n): y
Configuration saved to .migrator-env
```

### Main Menu

After configuration, you'll see the main menu:

```
MYSQLMIGRATOR - MySQL Database Migration Tool
=============================================
1. Overwrite Schema (Replace destination schema with master)
2. Update Schema (Add missing fields/tables only)
3. Migrate Data (Copy data from master to destination)
4. Reset Configuration
5. Exit
Enter your choice (1-5): 
```

### Migration Options

#### 1. Overwrite Schema

Completely replaces the schema in destination databases with the master schema. This includes:
- Dropping all existing tables
- Creating tables with the same structure as the master
- Migrating indexes, triggers, and stored procedures

#### 2. Update Schema

Updates the schema in destination databases without data loss:
- Adds missing tables
- Adds missing columns to existing tables
- Preserves existing data
- Updates indexes, triggers, and stored procedures

#### 3. Migrate Data

Copies data from master to destination databases:
- Option to specify a WHERE clause to filter data
- Handles data type conversions
- Manages collation differences

#### 4. Reset Configuration

Deletes the saved configuration file and runs the configuration wizard again.

## Advanced Features

### Collation Management

The tool automatically handles collation differences between databases:
- Detects source and destination collations
- Standardizes collations during table creation
- Handles utf8mb4 to latin1 conversion issues
- Falls back to utf8mb4_unicode_ci when needed

### Index Optimization

Automatically optimizes indexes for utf8mb4 character sets:
- Limits index lengths for text columns to avoid max key length errors
- Intelligently calculates required length limitations
- Provides fallback strategies for complex index scenarios

## Troubleshooting

### Collation Issues

If you encounter collation-related errors, the tool will attempt to resolve them automatically. If manual intervention is needed, you can:

```sql
ALTER TABLE `table_name` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### Index Length Issues

For index length issues on existing tables:

```sql
-- Drop problematic index
ALTER TABLE `table_name` DROP INDEX `index_name`;

-- Recreate with limited length
ALTER TABLE `table_name` ADD INDEX `index_name` (`column_name`(191));
```

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Author

Created by [@RIKIPB](https://github.com/RIKIPB) of [WeblyArts](https://www.weblyarts.com)
