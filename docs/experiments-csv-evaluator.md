# CSV Success Rate Evaluator

A powerful script for analyzing workflow execution data from CSV files and calculating success rates for FaaS-on-Spot experiments.

## Features

- 📊 **CSV Data Analysis**: Parse and analyze workflow execution data from CSV files
- 🔄 **Path-Aware Evaluation**: Support for both linear and branching workflows (wf-2, wf-6)
- 📈 **Comprehensive Metrics**: Success rates, execution counts, task completion statistics
- 🎯 **Flexible Filtering**: Analyze specific workflows, baselines, or all data
- 📤 **Multiple Output Formats**: Console output, CSV export, detailed reports
- 🔍 **Detailed Analysis**: Deep dive into individual workflow executions

## CSV Format

The script expects CSV files with the following columns:

```csv
workflow_name,workflow_stage,start_time,end_time,uuid_passed,response_code,restarted_at,baseline
wf-1,task1,1754945014.000,1754945028.000,a54b7703-e71a-45b2-ae16-d1c22a084ec9,200,NULL,static
wf-1,task2,1754945028.000,1754945043.000,a54b7703-e71a-45b2-ae16-d1c22a084ec9,200,NULL,static
```

### Required Columns:
- `workflow_name`: Name of the workflow (e.g., wf-1, wf-2)
- `workflow_stage`: Task name within the workflow
- `uuid_passed`: Unique identifier for each execution
- `baseline`: Baseline configuration name

### Optional Columns:
- `start_time`, `end_time`: Execution timing
- `response_code`: HTTP response code
- `restarted_at`: Restart information

## Usage

### Basic Usage

```bash
# Evaluate all workflows and baselines
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv

# Evaluate specific workflow
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --workflow wf-1

# Evaluate specific baseline
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --baseline static

# Evaluate specific workflow and baseline
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --workflow wf-1 --baseline static
```

### Advanced Options

```bash
# Show detailed execution analysis
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --workflow wf-1 --detailed

# Export results to CSV
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --output-csv results.csv

# Generate detailed report
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --output-report report.txt

# Use custom workflows directory
python3 evaluate_csv_success_rate.py --csv-file debugged-evals/resultset-evaluations-try2.csv --workflows-dir custom-workflows
```

## Command Line Arguments

| Argument | Description | Required | Default |
|----------|-------------|----------|---------|
| `--csv-file` | Path to CSV file to analyze | Yes | - |
| `--workflows-dir` | Directory containing workflow definitions | No | `workflows` |
| `--workflow` | Evaluate specific workflow only | No | All workflows |
| `--baseline` | Evaluate specific baseline only | No | All baselines |
| `--output-csv` | Export results to CSV file | No | - |
| `--output-report` | Generate detailed report file | No | - |
| `--detailed` | Show detailed execution analysis | No | False |
| `--use-paths` | Use path-aware evaluation (default) | No | True |

## Output Examples

### Console Summary
```
📊 SUCCESS RATE EVALUATION SUMMARY
================================================================================

🔹 Baseline: static
--------------------------------------------------
✅ wf-1: 85.7% (6/7)
🟡 wf-2: 75.0% (3/4)
🟠 wf-3: 45.5% (5/11)
```

### Detailed Analysis
```
🔍 DETAILED ANALYSIS FOR wf-1
============================================================
Total Executions: 7
Successful: 6
Failed: 1
Success Rate: 85.7%

Task Completion Statistics:
  task1: 7/7 (100.0%)
  task2: 6/7 (85.7%)
  task3: 6/7 (85.7%)
  task4: 6/7 (85.7%)
```

### CSV Export
The exported CSV contains:
- `workflow`: Workflow name
- `baseline`: Baseline configuration
- `total_executions`: Total number of executions
- `successful_executions`: Number of successful executions
- `failed_executions`: Number of failed executions
- `success_rate`: Success rate percentage

## Workflow Types Supported

### Linear Workflows (wf-1, wf-3, wf-4, wf-5, wf-7, wf-8, wf-9)
- **Success Criteria**: All tasks must complete
- **Example**: wf-1 requires task1, task2, task3, task4

### Branching Workflows (wf-2)
- **Success Criteria**: Common start tasks + at least one valid path
- **Valid Paths**: 
  - Path A: task3a + taskr1
  - Path B: task3b + taskr2
- **Required Start**: task1, task2, task3

### Special Workflows (wf-6)
- **Success Criteria**: All tasks must complete (corrected from circular dependency)

## Dependencies

```bash
pip3 install pandas
```

## Examples

### Example 1: Quick Analysis
```bash
# Analyze all data
python3 evaluate_csv_success_rate.py --csv-file experiment_results.csv
```

### Example 2: Specific Analysis
```bash
# Analyze wf-2 with detailed breakdown
python3 evaluate_csv_success_rate.py \
  --csv-file experiment_results.csv \
  --workflow wf-2 \
  --baseline snape \
  --detailed
```

### Example 3: Export Results
```bash
# Export all results to CSV and generate report
python3 evaluate_csv_success_rate.py \
  --csv-file experiment_results.csv \
  --output-csv success_rates.csv \
  --output-report analysis_report.txt
```

### Example 4: Custom Workflows Directory
```bash
# Use custom workflow definitions
python3 evaluate_csv_success_rate.py \
  --csv-file experiment_results.csv \
  --workflows-dir custom-workflows
```

## Testing

Run the test script to verify functionality:

```bash
python3 test_csv_evaluator.py
```

This will:
1. Test basic evaluation functionality
2. Show sample outputs
3. Export test results
4. Generate sample reports

## Integration with Existing Tools

The CSV evaluator can be used alongside your existing tools:

- **Database Export**: Export database results to CSV, then analyze with this tool
- **Batch Processing**: Process multiple CSV files in sequence
- **Report Generation**: Generate standardized reports for different experiments
- **Performance Tracking**: Track success rates over time

## Troubleshooting

### Common Issues

1. **"CSV file not found"**
   - Check the file path is correct
   - Ensure the file exists and is readable

2. **"Could not load workflow definition"**
   - Verify `workflows/` directory exists
   - Check `tasks.json` files are valid JSON

3. **"No data found"**
   - Verify CSV format matches expected structure
   - Check column names are correct

4. **Import errors**
   - Install required dependencies: `pip3 install pandas`
   - Ensure Python 3.7+ is used

### Debug Mode

For troubleshooting, you can modify the script to add debug output:

```python
# Add to CSVSuccessRateEvaluator.load_csv_data()
print(f"Debug: First few rows: {self.data[:3]}")
print(f"Debug: Columns: {list(self.data[0].keys()) if self.data else 'No data'}")
```

## Performance

- **Small files (< 1MB)**: Near-instantaneous
- **Medium files (1-10MB)**: 1-5 seconds
- **Large files (10MB+)**: 5-30 seconds depending on complexity

## License

This tool is part of the FaaS-on-Spot project and follows the same licensing terms.
