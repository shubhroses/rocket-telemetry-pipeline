#!/usr/bin/env python3
"""
Telemetry Data Ingestion and Cleaning Pipeline

Processes raw rocket engine telemetry data, performing validation, cleaning, and 
deduplication before outputting clean CSV data for analysis.

USAGE:
    python ingest_and_clean.py [options]
    
    Examples:
        # Process from stdin to default output
        python generate_telemetry.py 1000 | python ingest_and_clean.py
        
        # Process specific input file
        python ingest_and_clean.py -i raw_data.jsonl -o clean_data.csv
        
        # Process with custom output location
        cat telemetry.jsonl | python ingest_and_clean.py -o results/cleaned.csv

INPUT:
    - JSON Lines format (one JSON object per line)
    - Expected fields: timestamp, engine_id, chamber_pressure, fuel_flow, temperature
    - Reads from stdin by default or specified file with -i

OUTPUT:
    - CSV format with cleaned and validated data
    - Duplicate records removed (based on timestamp + engine_id)
    - Detailed processing logs to stderr and errors.log

CLEANING OPERATIONS:
    - Validates required fields (timestamp, engine_id)
    - Corrects negative chamber pressure → absolute value
    - Rejects temperatures below absolute zero (-273.15°C)
    - Fixes zero fuel flow → 0.1 kg/s minimum
    - Validates timestamp format (ISO format)
    - Removes duplicate records
    - Logs all corrections and errors

COMMAND LINE OPTIONS:
    -i, --input FILE     Input JSON Lines file (default: stdin)
    -o, --output FILE    Output CSV file (default: data/telemetry_clean.csv)

PURPOSE:
    Prepare raw telemetry data for analysis by cleaning common data quality issues
    while maintaining data integrity and providing detailed processing statistics.
"""

import json
import csv
import sys
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional, TextIO
from pathlib import Path


class TelemetryProcessor:
    def __init__(self, output_file: str = "data/telemetry_clean.csv"):
        self.output_file = output_file
        self.required_fields = ["timestamp", "engine_id"]
        self.numeric_fields = ["chamber_pressure", "fuel_flow", "temperature"]
        self.all_fields = self.required_fields + self.numeric_fields
        
        # Statistics
        self.stats = {
            "total_records": 0,
            "valid_records": 0,
            "dropped_records": 0,
            "corrected_records": 0,
            "parsing_errors": 0,
            "duplicate_records": 0
        }
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('errors.log'),
                logging.StreamHandler(sys.stderr)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate that record has required fields"""
        missing_fields = [field for field in self.required_fields if field not in record]
        
        if missing_fields:
            self.logger.warning(f"Record missing required fields: {missing_fields}. Record: {record}")
            return False
            
        return True
    
    def clean_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean and correct record issues"""
        cleaned_record = record.copy()
        corrected = False
        
        # Validate timestamp format
        try:
            timestamp_str = cleaned_record["timestamp"]
            # Try to parse timestamp to validate format
            datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, KeyError) as e:
            self.logger.error(f"Invalid timestamp format: {e}. Record: {record}")
            return None
            
        # Clean numeric fields
        for field in self.numeric_fields:
            if field in cleaned_record:
                try:
                    value = float(cleaned_record[field])
                    
                    # Fix negative pressure (absolute value)
                    if field == "chamber_pressure" and value < 0:
                        cleaned_record[field] = abs(value)
                        corrected = True
                        self.logger.warning(f"Corrected negative pressure: {value} → {abs(value)}")
                    
                    # Fix unrealistic temperature values
                    elif field == "temperature":
                        if value < -273.15:  # Below absolute zero
                            self.logger.error(f"Temperature below absolute zero: {value}. Dropping record.")
                            return None
                        elif value > 6000:  # Unrealistically high for rocket engines
                            self.logger.warning(f"Extremely high temperature detected: {value}")
                    
                    # Fix zero fuel flow (set to minimum realistic value)
                    elif field == "fuel_flow" and value == 0:
                        cleaned_record[field] = 0.1  # Minimum flow
                        corrected = True
                        self.logger.warning(f"Corrected zero fuel flow to 0.1 kg/s")
                        
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Invalid numeric value for {field}: {e}. Record: {record}")
                    return None
        
        if corrected:
            self.stats["corrected_records"] += 1
            
        return cleaned_record
    
    def process_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Process a single line of JSON input"""
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#'):
            return None
            
        try:
            record = json.loads(line)
            self.stats["total_records"] += 1
            
            # Validate required fields
            if not self.validate_record(record):
                self.stats["dropped_records"] += 1
                return None
                
            # Clean and correct issues
            cleaned_record = self.clean_record(record)
            if cleaned_record is None:
                self.stats["dropped_records"] += 1
                return None
                
            self.stats["valid_records"] += 1
            return cleaned_record
            
        except json.JSONDecodeError as e:
            self.stats["parsing_errors"] += 1
            self.logger.error(f"JSON parsing error: {e}. Line: {line[:100]}...")
            return None
    
    def process_stream(self, input_stream: TextIO) -> None:
        """Process telemetry data from input stream"""
        self.logger.info(f"Starting telemetry data processing...")
        self.logger.info(f"Output file: {self.output_file}")
        
        # Ensure output directory exists
        Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)
        
        processed_records = []
        
        try:
            for line_num, line in enumerate(input_stream, 1):
                cleaned_record = self.process_line(line)
                if cleaned_record:
                    processed_records.append(cleaned_record)
                    
        except Exception as e:
            self.logger.error(f"Unexpected error processing line {line_num}: {e}")
            
        # Write to CSV
        self.write_csv(processed_records)
        self.log_summary()
    
    def write_csv(self, records: List[Dict[str, Any]]) -> None:
        """Write cleaned records to CSV file"""
        if not records:
            self.logger.warning("No valid records to write to CSV")
            return
        
        # Remove duplicates based on key fields (timestamp + engine_id)
        seen_records = set()
        unique_records = []
        duplicate_count = 0
        
        for record in records:
            # Create a unique key from timestamp and engine_id
            key = (record.get('timestamp'), record.get('engine_id'))
            
            if key not in seen_records:
                seen_records.add(key)
                unique_records.append(record)
            else:
                duplicate_count += 1
                self.logger.warning(f"Duplicate record removed: {record.get('timestamp')} - {record.get('engine_id')}")
        
        if duplicate_count > 0:
            self.logger.info(f"Removed {duplicate_count} duplicate records")
            self.stats["duplicate_records"] = duplicate_count
            
        try:
            with open(self.output_file, 'w', newline='') as csvfile:
                # Use all possible fields as header
                fieldnames = self.all_fields
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for record in unique_records:
                    # Ensure all fields are present (fill missing with None)
                    row = {field: record.get(field) for field in fieldnames}
                    writer.writerow(row)
                    
            self.logger.info(f"Successfully wrote {len(unique_records)} unique records to {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Error writing CSV file: {e}")
            raise
    
    def log_summary(self) -> None:
        """Log processing summary statistics"""
        self.logger.info("=" * 50)
        self.logger.info("PROCESSING SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Total records processed: {self.stats['total_records']}")
        self.logger.info(f"Valid records: {self.stats['valid_records']}")
        self.logger.info(f"Dropped records: {self.stats['dropped_records']}")
        self.logger.info(f"Corrected records: {self.stats['corrected_records']}")
        self.logger.info(f"Duplicate records removed: {self.stats.get('duplicate_records', 0)}")
        self.logger.info(f"Parsing errors: {self.stats['parsing_errors']}")
        
        if self.stats['total_records'] > 0:
            success_rate = (self.stats['valid_records'] / self.stats['total_records']) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")
            
            # Calculate final output rate
            final_records = self.stats['valid_records'] - self.stats.get('duplicate_records', 0)
            output_rate = (final_records / self.stats['total_records']) * 100
            self.logger.info(f"Final output rate: {output_rate:.1f}% ({final_records} unique records)")


def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description="Clean and validate rocket engine telemetry data")
    parser.add_argument(
        "-i", "--input", 
        type=str, 
        help="Input JSON file (default: stdin)"
    )
    parser.add_argument(
        "-o", "--output", 
        type=str, 
        default="data/telemetry_clean.csv",
        help="Output CSV file (default: data/telemetry_clean.csv)"
    )
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = TelemetryProcessor(output_file=args.output)
    
    # Process input
    try:
        if args.input:
            with open(args.input, 'r') as input_file:
                processor.process_stream(input_file)
        else:
            processor.process_stream(sys.stdin)
            
    except FileNotFoundError:
        print(f"Error: Input file '{args.input}' not found.", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main() 