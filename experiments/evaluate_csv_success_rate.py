#!/usr/bin/env python3
"""
CSV Success Rate Evaluator for FaaS-on-Spot Experiments

This script analyzes CSV files containing workflow execution data to calculate
success rates for different baselines and workflows. It supports both linear
and branching workflows with path-aware success evaluation.

Usage:
    python3 evaluate_csv_success_rate.py --csv-file <file> [options]
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd

class CSVSuccessRateEvaluator:
    """Evaluates success rates from CSV files containing workflow execution data."""
    
    def __init__(self, csv_file: str, workflows_dir: str = "workflows"):
        self.csv_file = csv_file
        self.workflows_dir = workflows_dir
        self.data = []
        self.workflows_data = {}
        self.load_csv_data()
        self.load_workflow_definitions()
    
    def load_csv_data(self):
        """Load and parse the CSV file."""
        try:
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                self.data = list(reader)
            print(f"✅ Loaded {len(self.data)} records from {self.csv_file}")
        except Exception as e:
            print(f"❌ Error loading CSV file: {e}")
            sys.exit(1)
    
    def load_workflow_definitions(self):
        """Load workflow definitions from tasks.json files."""
        for wf in os.listdir(self.workflows_dir):
            if not wf.startswith("wf-"):
                continue
            
            wf_path = os.path.join(self.workflows_dir, wf, "tasks.json")
            if os.path.exists(wf_path):
                try:
                    with open(wf_path, 'r') as f:
                        wf_data = json.load(f)
                        self.workflows_data[wf] = wf_data["tasks"]
                    print(f"✅ Loaded workflow definition for {wf}")
                except Exception as e:
                    print(f"⚠️ Warning: Could not load workflow definition for {wf}: {e}")
    
    def get_workflow_tasks(self, workflow_name: str) -> Dict:
        """Get task definitions for a specific workflow."""
        return self.workflows_data.get(workflow_name, {})
    
    def is_valid_execution_path(self, workflow_name: str, completed_tasks: Set[str]) -> bool:
        """Check if a set of completed tasks represents a valid execution path."""
        if workflow_name not in self.workflows_data:
            return False
        
        tasks_data = self.workflows_data[workflow_name]
        task_ids = set(tasks_data.keys())
        
        # Check if all required start tasks are completed
        if workflow_name == "wf-1":
            # wf-1 is a branching workflow
            required_start = {"task1", "task2", "task3"}
            if not required_start.issubset(completed_tasks):
                return False
            
            # Check if at least one valid path is completed
            path_a = {"task3a", "taskr1"}
            path_b = {"task3b", "taskr2"}
            
            if path_a.issubset(completed_tasks) or path_b.issubset(completed_tasks):
                return True
            return False
        
        elif workflow_name == "wf-5":
            # wf-5 is a linear workflow that requires all tasks
            return completed_tasks == task_ids
        
        else:
            # Default: linear workflow requiring all tasks
            return completed_tasks == task_ids
    
    def evaluate_workflow_success(self, workflow_name: str, baseline: Optional[str] = None) -> Dict:
        """Evaluate success rate for a specific workflow and baseline."""
        # Filter data by workflow and baseline
        filtered_data = [
            row for row in self.data 
            if row['workflow_name'] == workflow_name and 
               (baseline is None or row['baseline'] == baseline)
        ]
        
        if not filtered_data:
            return {
                'workflow': workflow_name,
                'baseline': baseline if baseline else 'all',
                'total_executions': 0,
                'successful_executions': 0,
                'success_rate': 0.0,
                'failed_executions': 0
            }
        
        # Group by UUID to analyze each execution
        executions = defaultdict(set)
        for row in filtered_data:
            uuid = row['uuid_passed']
            task = row['workflow_stage']
            executions[uuid].add(task)
        
        # Evaluate each execution
        successful_executions = 0
        failed_executions = 0
        
        for uuid, completed_tasks in executions.items():
            if self.is_valid_execution_path(workflow_name, completed_tasks):
                successful_executions += 1
            else:
                failed_executions += 1
        
        total_executions = len(executions)
        success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
        
        return {
            'workflow': workflow_name,
            'baseline': baseline if baseline else 'all',
            'total_executions': total_executions,
            'successful_executions': successful_executions,
            'failed_executions': failed_executions,
            'success_rate': round(success_rate, 2)
        }
    
    def evaluate_all_workflows(self, baseline: Optional[str] = None) -> List[Dict]:
        """Evaluate success rates for all workflows."""
        results = []
        workflows = set(row['workflow_name'] for row in self.data if row['workflow_name'].startswith('wf-'))
        
        for workflow in sorted(workflows):
            result = self.evaluate_workflow_success(workflow, baseline)
            results.append(result)
        
        return results
    
    def evaluate_all_baselines(self) -> List[Dict]:
        """Evaluate success rates for all baselines across all workflows."""
        results = []
        baselines = set(row['baseline'] for row in self.data if row['baseline'] != 'baseline')
        
        for baseline in sorted(baselines):
            baseline_results = self.evaluate_all_workflows(baseline)
            results.extend(baseline_results)
        
        return results
    
    def get_detailed_execution_analysis(self, workflow_name: str, baseline: Optional[str] = None) -> Dict:
        """Get detailed analysis of executions for a workflow."""
        filtered_data = [
            row for row in self.data 
            if row['workflow_name'] == workflow_name and 
               (baseline is None or row['baseline'] == baseline)
        ]
        
        if not filtered_data:
            return {}
        
        # Group by UUID
        executions: Dict[str, Dict] = defaultdict(lambda: {'tasks': set(), 'start_time': None, 'end_time': None})
        
        for row in filtered_data:
            uuid = row['uuid_passed']
            if executions[uuid]['tasks'] is not None:
                executions[uuid]['tasks'].add(row['workflow_stage'])
            
            # Track timing
            start_time = float(row['start_time'])
            end_time = float(row['end_time'])
            
            if executions[uuid]['start_time'] is None or start_time < executions[uuid]['start_time']:
                executions[uuid]['start_time'] = start_time
            if executions[uuid]['end_time'] is None or end_time > executions[uuid]['end_time']:
                executions[uuid]['end_time'] = end_time
        
        # Analyze each execution
        analysis = {
            'total_executions': len(executions),
            'successful_executions': 0,
            'failed_executions': 0,
            'execution_details': [],
            'task_completion_stats': defaultdict(int)
        }
        
        for uuid, exec_data in executions.items():
            if exec_data['tasks'] is not None:
                is_successful = self.is_valid_execution_path(workflow_name, exec_data['tasks'])
                
                if is_successful:
                    analysis['successful_executions'] += 1
                else:
                    analysis['failed_executions'] += 1
                
                # Track task completion statistics
                for task in exec_data['tasks']:
                    analysis['task_completion_stats'][task] += 1
                
                # Add execution detail
                duration = exec_data['end_time'] - exec_data['start_time'] if exec_data['end_time'] and exec_data['start_time'] else 0
                analysis['execution_details'].append({
                    'uuid': uuid,
                    'completed_tasks': list(exec_data['tasks']),
                    'is_successful': is_successful,
                    'duration': round(duration, 2),
                    'start_time': exec_data['start_time'],
                    'end_time': exec_data['end_time']
                })
        
        return analysis
    
    def print_summary(self, results: List[Dict]):
        """Print a formatted summary of results."""
        print("\n" + "="*80)
        print("📊 SUCCESS RATE EVALUATION SUMMARY")
        print("="*80)
        
        # Group by baseline
        by_baseline = defaultdict(list)
        for result in results:
            by_baseline[result['baseline']].append(result)
        
        for baseline, baseline_results in sorted(by_baseline.items()):
            print(f"\n🔹 Baseline: {baseline}")
            print("-" * 50)
            
            for result in baseline_results:
                status_emoji = "✅" if result['success_rate'] >= 90 else "🟡" if result['success_rate'] >= 70 else "🟠"
                print(f"{status_emoji} {result['workflow']}: {result['success_rate']}% "
                      f"({result['successful_executions']}/{result['total_executions']})")
    
    def export_results(self, results: List[Dict], output_file: str):
        """Export results to a CSV file."""
        try:
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False)
            print(f"✅ Results exported to {output_file}")
        except Exception as e:
            print(f"❌ Error exporting results: {e}")
    
    def generate_report(self, results: List[Dict], output_file: Optional[str] = None):
        """Generate a comprehensive report."""
        report_lines = []
        report_lines.append("FaaS-on-Spot Success Rate Evaluation Report")
        report_lines.append("=" * 50)
        report_lines.append(f"Source CSV: {self.csv_file}")
        report_lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Group by baseline
        by_baseline = defaultdict(list)
        for result in results:
            by_baseline[result['baseline']].append(result)
        
        for baseline, baseline_results in sorted(by_baseline.items()):
            report_lines.append(f"Baseline: {baseline}")
            report_lines.append("-" * 30)
            
            for result in baseline_results:
                status = "SUCCESS" if result['success_rate'] >= 90 else "PARTIAL" if result['success_rate'] >= 70 else "FAILED"
                report_lines.append(f"  {result['workflow']}: {result['success_rate']}% ({status})")
                report_lines.append(f"    Executions: {result['successful_executions']} successful, {result['failed_executions']} failed")
                report_lines.append("")
        
        # Write report
        if output_file:
            with open(output_file, 'w') as f:
                f.write('\n'.join(report_lines))
            print(f"✅ Report generated: {output_file}")
        else:
            print('\n'.join(report_lines))

def main():
    parser = argparse.ArgumentParser(description="Evaluate success rates from CSV workflow data")
    parser.add_argument("--csv-file", required=True, help="Path to the CSV file to analyze")
    parser.add_argument("--workflows-dir", default="workflows", help="Directory containing workflow definitions")
    parser.add_argument("--workflow", help="Evaluate specific workflow only")
    parser.add_argument("--baseline", help="Evaluate specific baseline only")
    parser.add_argument("--output-csv", help="Export results to CSV file")
    parser.add_argument("--output-report", help="Generate detailed report file")
    parser.add_argument("--detailed", action="store_true", help="Show detailed execution analysis")
    parser.add_argument("--use-paths", action="store_true", help="Use path-aware evaluation (default)")
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = CSVSuccessRateEvaluator(args.csv_file, args.workflows_dir)
    
    # Run evaluation
    if args.workflow:
        # Single workflow evaluation
        if args.baseline:
            result = evaluator.evaluate_workflow_success(args.workflow, args.baseline)
            results = [result]
        else:
            results = evaluator.evaluate_all_workflows()
            results = [r for r in results if r['workflow'] == args.workflow]
    elif args.baseline:
        # Single baseline evaluation
        results = evaluator.evaluate_all_workflows(args.baseline)
    else:
        # All workflows and baselines
        results = evaluator.evaluate_all_baselines()
    
    # Print summary
    evaluator.print_summary(results)
    
    # Show detailed analysis if requested
    if args.detailed and args.workflow:
        print(f"\n🔍 DETAILED ANALYSIS FOR {args.workflow}")
        print("="*60)
        analysis = evaluator.get_detailed_execution_analysis(args.workflow, args.baseline)
        
        if analysis:
            print(f"Total Executions: {analysis['total_executions']}")
            print(f"Successful: {analysis['successful_executions']}")
            print(f"Failed: {analysis['failed_executions']}")
            print(f"Success Rate: {analysis['successful_executions']/analysis['total_executions']*100:.1f}%")
            
            print(f"\nTask Completion Statistics:")
            for task, count in sorted(analysis['task_completion_stats'].items()):
                percentage = count / analysis['total_executions'] * 100
                print(f"  {task}: {count}/{analysis['total_executions']} ({percentage:.1f}%)")
    
    # Export results if requested
    if args.output_csv:
        evaluator.export_results(results, args.output_csv)
    
    if args.output_report:
        evaluator.generate_report(results, args.output_report)
    
    # Print individual results
    print(f"\n📋 DETAILED RESULTS:")
    print("-" * 60)
    for result in results:
        status_emoji = "✅" if result['success_rate'] >= 90 else "🟡" if result['success_rate'] >= 70 else "🟠"
        print(f"{status_emoji} {result['workflow']} | {result['baseline']}: "
              f"{result['success_rate']}% ({result['successful_executions']}/{result['total_executions']})")

if __name__ == "__main__":
    main()
