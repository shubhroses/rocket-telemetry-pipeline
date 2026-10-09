#!/usr/bin/env python3
"""
Rocket Engine Telemetry Data Generator

Generates synthetic rocket engine telemetry data for testing data quality pipelines.

USAGE:
    python generate_telemetry.py [num_records]

    Examples:
        python generate_telemetry.py 100        # Generate 100 records
        python generate_telemetry.py 1000 > data.jsonl  # Save 1000 records to file

OUTPUT:
    - JSON Lines format (one JSON object per line) to stdout
    - Each record contains: timestamp, engine_id, chamber_pressure, fuel_flow, temperature
    - Intentional data quality issues included for pipeline testing

FEATURES:
    - 5 simulated rocket engines with different performance characteristics
    - Gaussian noise on every measurement. Pressure and fuel flow rise with an
      engine's performance factor and temperature falls with it
    - Fault injection at the fixed rates in TelemetryGenerator.anomaly_rates:
        * Missing sensor readings (simulates sensor failures), about 3% of readings
        * Out-of-range values (simulates sensor malfunctions), about 5%
        * Duplicate records (simulates data pipeline issues), about 4%
        * Critical engine failures (pressure spikes, thermal runaway, etc.),
          0.16% to 0.36% depending on the engine

PURPOSE:
    Provide input with known kinds of faults for testing data quality validation,
    anomaly detection and the rest of the pipeline.
"""

import json
import random
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List


class TelemetryGenerator:
    def __init__(self):
        # Engine configurations with moderate differences
        self.engines = {
            "ENG-001": {"performance": 0.88, "failure_rate": 0.12, "name": "Engine Alpha"},  # Good condition
            "ENG-002": {"performance": 0.75, "failure_rate": 0.18, "name": "Engine Beta"},  # Needs attention
            "ENG-003": {"performance": 0.92, "failure_rate": 0.08, "name": "Engine Gamma"},  # Excellent
            "ENG-004": {"performance": 0.82, "failure_rate": 0.15, "name": "Engine Delta"},  # Good condition
            "ENG-005": {"performance": 0.85, "failure_rate": 0.13, "name": "Engine Epsilon"},  # Good condition
        }

        # Parameter ranges
        self.base_params = {
            "chamber_pressure": {"min": 150, "max": 300, "unit": "psi"},
            "fuel_flow": {"min": 50, "max": 150, "unit": "kg/s"},
            "temperature": {"min": 2000, "max": 4000, "unit": "°F"},
        }

        # Fault injection rates
        self.anomaly_rates = {
            "missing_fields": 0.03,  # 3% missing data (sensor failures)
            "out_of_range": 0.05,  # 5% sensor errors
            "duplicates": 0.04,  # 4% duplicate readings
            "critical_failures": 0.02,  # times the engine's failure_rate: 0.16% to 0.36% of readings
        }

    def generate_base_reading(self, engine_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Generate one telemetry reading for an engine"""
        engine_config = self.engines[engine_id]
        performance_factor = engine_config["performance"]

        # Base readings adjusted by engine performance
        reading = {
            "timestamp": timestamp.isoformat(),
            "engine_id": engine_id,
            "chamber_pressure": self._generate_pressure(performance_factor),
            "fuel_flow": self._generate_fuel_flow(performance_factor),
            "temperature": self._generate_temperature(performance_factor),
        }

        return reading

    def _generate_pressure(self, performance_factor: float) -> float:
        """Generate chamber pressure with performance degradation"""
        base_range = self.base_params["chamber_pressure"]
        optimal_pressure = base_range["min"] + (base_range["max"] - base_range["min"]) * performance_factor

        # Add Gaussian sensor noise
        noise = random.normalvariate(0, 10)
        return max(0, optimal_pressure + noise)

    def _generate_fuel_flow(self, performance_factor: float) -> float:
        """Generate fuel flow with efficiency factors"""
        base_range = self.base_params["fuel_flow"]
        optimal_flow = base_range["min"] + (base_range["max"] - base_range["min"]) * performance_factor

        # Add sensor noise and efficiency variations
        noise = random.normalvariate(0, 8)
        return max(0, optimal_flow + noise)

    def _generate_temperature(self, performance_factor: float) -> float:
        """Generate temperature with performance correlation"""
        base_range = self.base_params["temperature"]

        # Poor performance = higher temperatures (inefficient combustion)
        temp_factor = 1.0 + (0.3 * (1 - performance_factor))
        optimal_temp = base_range["min"] + (base_range["max"] - base_range["min"]) * temp_factor

        # Add Gaussian sensor noise
        noise = random.normalvariate(0, 50)
        return max(500, optimal_temp + noise)

    def inject_anomalies(self, reading: Dict[str, Any]) -> Dict[str, Any]:
        """Inject various types of anomalies for testing"""
        engine_id = reading["engine_id"]
        failure_rate = self.engines[engine_id]["failure_rate"]

        # Critical engine failures (based on engine condition)
        if random.random() < failure_rate * self.anomaly_rates["critical_failures"]:
            return self._inject_critical_failure(reading)

        # Missing fields anomaly
        if random.random() < self.anomaly_rates["missing_fields"]:
            return self._inject_missing_fields(reading)

        # Out of range values
        if random.random() < self.anomaly_rates["out_of_range"]:
            return self._inject_out_of_range(reading)

        return reading

    def _inject_critical_failure(self, reading: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate critical engine failures"""
        failure_type = random.choice(
            ["fuel_system_failure", "pressure_spike", "temperature_runaway", "combustion_instability"]
        )

        if failure_type == "fuel_system_failure":
            reading["fuel_flow"] = 0  # Fuel pump failure
        elif failure_type == "pressure_spike":
            reading["chamber_pressure"] = random.uniform(400, 600)  # Dangerous overpressure
        elif failure_type == "temperature_runaway":
            reading["temperature"] = random.uniform(5000, 8000)  # Thermal runaway
        elif failure_type == "combustion_instability":
            reading["chamber_pressure"] = random.uniform(-50, 50)  # Unstable combustion

        return reading

    def _inject_missing_fields(self, reading: Dict[str, Any]) -> Dict[str, Any]:
        """Remove random fields to simulate sensor failures"""
        fields_to_remove = random.sample(["chamber_pressure", "fuel_flow", "temperature"], random.randint(1, 2))

        anomaly_reading = reading.copy()
        for field in fields_to_remove:
            del anomaly_reading[field]

        return anomaly_reading

    def _inject_out_of_range(self, reading: Dict[str, Any]) -> Dict[str, Any]:
        """Generate unrealistic sensor readings"""
        field = random.choice(["chamber_pressure", "fuel_flow", "temperature"])

        if field == "chamber_pressure":
            reading[field] = random.choice(
                [
                    random.uniform(-100, -10),  # Negative pressure (impossible)
                    random.uniform(500, 1000),  # Extreme overpressure
                ]
            )
        elif field == "fuel_flow":
            reading[field] = random.choice(
                [
                    0,  # Zero flow (engine stall)
                    random.uniform(300, 500),  # Impossible high flow
                ]
            )
        elif field == "temperature":
            reading[field] = random.choice(
                [
                    random.uniform(-300, 0),  # Below absolute zero
                    random.uniform(8000, 12000),  # Impossibly high
                ]
            )

        return reading

    def generate_telemetry_batch(self, num_records: int) -> List[Dict[str, Any]]:
        """Generate a batch of telemetry records, each 1-5 seconds after the previous one"""
        records = []
        duplicates_to_add = []

        # Start with current time and progress chronologically
        current_timestamp = datetime.now()

        for i in range(num_records):
            # Time progression - each reading 1-5 seconds after the previous
            if i > 0:  # Skip time advancement for first record
                time_interval = random.uniform(1, 5)  # Random interval between readings
                current_timestamp += timedelta(seconds=time_interval)

            # Select engine (balanced operational status)
            engine_weights = [0.2, 0.2, 0.2, 0.2, 0.2]  # Equal distribution across all engines
            engine_id = random.choices(list(self.engines.keys()), weights=engine_weights)[0]

            # Generate base reading
            reading = self.generate_base_reading(engine_id, current_timestamp)

            # Inject anomalies
            reading = self.inject_anomalies(reading)

            records.append(reading)

            # Create duplicates for testing
            if random.random() < self.anomaly_rates["duplicates"]:
                duplicates_to_add.append(reading.copy())

        # Add duplicates to simulate data pipeline issues
        records.extend(duplicates_to_add)

        # Note: Records maintain chronological order for easier analysis
        # In production, data may arrive out-of-order due to network conditions

        return records


def main():
    """Generate telemetry data and output to stdout"""
    if len(sys.argv) > 1:
        try:
            num_records = int(sys.argv[1])
        except ValueError:
            print("Error: Number of records must be an integer", file=sys.stderr)
            sys.exit(1)
    else:
        num_records = 100

    generator = TelemetryGenerator()

    print(f"# Generating {num_records} telemetry records...", file=sys.stderr)
    print(f"# Engine IDs: {list(generator.engines.keys())}", file=sys.stderr)
    print(f"# Started at: {datetime.now().isoformat()}", file=sys.stderr)
    print(
        f"# Expected anomalies: ~{generator.anomaly_rates['missing_fields'] * 100:.0f}% missing fields, ~{generator.anomaly_rates['out_of_range'] * 100:.0f}% out-of-range values, ~{generator.anomaly_rates['duplicates'] * 100:.0f}% duplicates",
        file=sys.stderr,
    )
    print(
        f"# Moderate failure rates: ~{generator.anomaly_rates['critical_failures'] * 100:.0f}% critical failures",
        file=sys.stderr,
    )

    records = generator.generate_telemetry_batch(num_records)

    for record in records:
        print(json.dumps(record))

    print(f"# Generated {len(records)} records successfully!", file=sys.stderr)


if __name__ == "__main__":
    main()
