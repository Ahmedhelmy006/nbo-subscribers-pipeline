#!/usr/bin/env python3
"""
Script to retrieve database table column information.
"""
import os
import sys
import asyncio

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from db.connection import db_manager

async def get_table_columns():
    """
    Retrieve column information for the subscribers table.
    """
    query = """
    SELECT 
        column_name, 
        data_type, 
        character_maximum_length,
        is_nullable
    FROM 
        information_schema.columns
    WHERE 
        table_name = 'subscribers'
    ORDER BY 
        ordinal_position
    """
    
    try:
        # Initialize database connection
        await db_manager.initialize()
        
        # Fetch columns
        columns = await db_manager.fetch(query)
        
        # Prepare output file path
        output_file = os.path.join(project_root, 'subscribers_columns.txt')
        
        # Write columns to file
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("Column Name,Data Type,Max Length,Nullable\n")
            
            # Write each column's details
            for column in columns:
                f.write(f"{column['column_name']},"
                        f"{column['data_type']},"
                        f"{column['character_maximum_length'] or 'N/A'},"
                        f"{column['is_nullable']}\n")
        
        print(f"Column information written to {output_file}")
        
    except Exception as e:
        print(f"Error retrieving column information: {e}")
    finally:
        # Close database connection
        await db_manager.close()

async def main():
    await get_table_columns()

if __name__ == "__main__":
    asyncio.run(main())