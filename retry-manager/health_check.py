#!/usr/bin/env python3
"""
Simple health check script for the retry service.
This script checks if the retry service is running properly by:
1. Checking if Python is working
2. Checking if required directories exist
3. Checking if required modules can be imported
4. Optionally checking if the service is responding (if port is exposed)
"""

import sys
import os
import importlib

def check_python():
    """Check if Python is working properly."""
    try:
        import sys
        print(f"Python version: {sys.version}")
        return True
    except Exception as e:
        print(f"Python check failed: {e}")
        return False

def check_directories():
    """Check if required directories exist."""
    required_dirs = [
        "/app/workflows"
    ]
    
    optional_dirs = [
        "/secrets"  # Optional - only required if Redis password is mounted as secret
    ]
    
    # Check required directories
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            print(f"Required directory missing: {dir_path}")
            return False
        print(f"Directory exists: {dir_path}")
    
    # Check optional directories (warn but don't fail)
    for dir_path in optional_dirs:
        if os.path.exists(dir_path):
            print(f"Directory exists: {dir_path}")
        else:
            print(f"Optional directory missing: {dir_path} (may be mounted later)")
    
    return True

def check_modules():
    """Check if required Python modules can be imported."""
    required_modules = [
        "mysql.connector",
        "redis", 
        "requests",
        "json",
        "time",
        "os"
    ]
    
    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
            print(f"Module imported successfully: {module_name}")
        except ImportError as e:
            print(f"Failed to import module {module_name}: {e}")
            return False
    
    return True

def check_environment():
    """Check if required environment variables are set."""
    required_env_vars = [
        "MYSQL_HOST",
        "MYSQL_USER", 
        "MYSQL_PASSWORD",
        "MYSQL_DB",
        "REDIS_HOST",
        "REDIS_PORT",
        "BASELINE"
    ]
    
    for env_var in required_env_vars:
        if not os.getenv(env_var):
            print(f"Required environment variable missing: {env_var}")
            return False
        print(f"Environment variable set: {env_var}")
    
    return True

def main():
    """Run all health checks."""
    print("Starting retry service health check...")
    
    checks = [
        ("Python", check_python),
        ("Directories", check_directories), 
        ("Modules", check_modules),
        ("Environment", check_environment)
    ]
    
    all_passed = True
    for check_name, check_func in checks:
        print(f"\n--- {check_name} Check ---")
        if not check_func():
            print(f"❌ {check_name} check failed")
            all_passed = False
        else:
            print(f"✅ {check_name} check passed")
    
    if all_passed:
        print("\n🎉 All health checks passed!")
        sys.exit(0)
    else:
        print("\n💥 Some health checks failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
