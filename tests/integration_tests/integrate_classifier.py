"""
Test the email classifier on a large dataset of subscribers.

This script processes a JSON file of subscribers, classifies all emails,
and writes the results to CSV files for analysis.
"""
import os
import sys
import json
import csv
import time
from pathlib import Path
from datetime import datetime
from collections import OrderedDict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import email classifier
from email_classifier.classifier import classify_email

def collect_all_fields(subscribers):
    """
    Collect all possible fields from all subscriber records.
    
    Args:
        subscribers: List of subscriber dictionaries
    
    Returns:
        List of all field names
    """
    all_fields = set()
    
    # Sample a subset for performance if very large dataset
    sample_size = min(len(subscribers), 10000)
    for subscriber in subscribers[:sample_size]:
        all_fields.update(subscriber.keys())
    
    # Prioritize certain fields
    ordered_fields = []
    
    # These fields should come first in this order
    priority_fields = ["id", "email_address", "first_name", "last_name"]
    for field in priority_fields:
        if field in all_fields:
            ordered_fields.append(field)
            all_fields.remove(field)
    
    # Add the rest of the fields
    ordered_fields.extend(sorted(all_fields))
    
    return ordered_fields

def process_subscribers_file(input_file, output_dir):
    """
    Process a JSON file of subscribers and classify all emails.
    
    Args:
        input_file: Path to the input JSON file
        output_dir: Directory to write output files
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    work_csv = output_path / f"work_emails_{timestamp}.csv"
    personal_csv = output_path / f"personal_emails_{timestamp}.csv"
    unknown_csv = output_path / f"unknown_emails_{timestamp}.csv"
    stats_file = output_path / f"classification_stats_{timestamp}.txt"
    
    # Load the JSON file
    print(f"Loading subscribers from {input_file}...")
    start_time = time.time()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            subscribers = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return
    
    load_time = time.time() - start_time
    print(f"Loaded {len(subscribers)} subscribers in {load_time:.2f} seconds")
    
    # Initialize counters
    total_count = len(subscribers)
    work_count = 0
    personal_count = 0
    unknown_count = 0
    
    # Collect all fields from all records
    print("Collecting all field names...")
    all_fields = collect_all_fields(subscribers)
    
    # Add classification fields that we'll be adding
    all_fields.extend(["email_type", "domain"])
    
    print(f"Found {len(all_fields)} unique fields")
    
    # Set up CSV writers
    with open(work_csv, 'w', newline='', encoding='utf-8') as work_file, \
         open(personal_csv, 'w', newline='', encoding='utf-8') as personal_file, \
         open(unknown_csv, 'w', newline='', encoding='utf-8') as unknown_file:
        
        # Create CSV writers
        work_writer = csv.DictWriter(work_file, fieldnames=all_fields, extrasaction='ignore')
        personal_writer = csv.DictWriter(personal_file, fieldnames=all_fields, extrasaction='ignore')
        unknown_writer = csv.DictWriter(unknown_file, fieldnames=all_fields, extrasaction='ignore')
        
        # Write headers
        work_writer.writeheader()
        personal_writer.writeheader()
        unknown_writer.writeheader()
        
        # Process each subscriber
        print(f"Classifying emails...")
        classification_start = time.time()
        
        for i, subscriber in enumerate(subscribers):
            # Progress update every 10,000 records
            if (i + 1) % 10000 == 0:
                elapsed = time.time() - classification_start
                rate = (i + 1) / elapsed
                remaining = (total_count - (i + 1)) / rate
                print(f"Progress: {i+1}/{total_count} ({(i+1)/total_count*100:.1f}%) - "
                      f"Rate: {rate:.1f} emails/sec - "
                      f"Remaining: {remaining/60:.1f} minutes")
            
            # Get email
            email = subscriber.get("email_address", "")
            
            # Classify email
            email_type, domain = classify_email(email)
            
            # Add classification to subscriber
            subscriber_copy = subscriber.copy()
            subscriber_copy["email_type"] = email_type
            subscriber_copy["domain"] = domain
            
            # Write to appropriate CSV
            if email_type == "work":
                work_writer.writerow(subscriber_copy)
                work_count += 1
            elif email_type == "personal":
                personal_writer.writerow(subscriber_copy)
                personal_count += 1
            else:
                unknown_writer.writerow(subscriber_copy)
                unknown_count += 1
    
    # Calculate statistics
    total_time = time.time() - start_time
    classification_time = time.time() - classification_start
    
    stats = [
        f"Email Classification Results",
        f"-------------------------",
        f"Total subscribers: {total_count}",
        f"Work emails: {work_count} ({work_count/total_count*100:.1f}%)",
        f"Personal emails: {personal_count} ({personal_count/total_count*100:.1f}%)",
        f"Unknown emails: {unknown_count} ({unknown_count/total_count*100:.1f}%)",
        f"",
        f"Performance",
        f"-------------------------",
        f"Total processing time: {total_time:.2f} seconds",
        f"Classification time: {classification_time:.2f} seconds",
        f"Classification rate: {total_count/classification_time:.1f} emails/second",
    ]
    
    # Write statistics to file
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(stats))
    
    # Print summary
    print("\n".join(stats))
    print(f"\nResults written to:")
    print(f"Work emails: {work_csv}")
    print(f"Personal emails: {personal_csv}")
    print(f"Unknown emails: {unknown_csv}")
    print(f"Statistics: {stats_file}")

if __name__ == "__main__":
    # Use a simple approach without argparse for clarity
    # Default values
    input_file = r"D:\Subscribers Pipeline\output\all_subscribers_final.json"
    output_dir = r"D:\database_pipeline\classification_results"
    
    # Check command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    
    # Process the file
    process_subscribers_file(input_file, output_dir)