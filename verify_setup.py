"""
Test script to verify the pipeline setup step by step
"""
import os
import sys

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def check(name, passed):
    if passed:
        print(f"{Colors.GREEN}✓{Colors.END} {name}")
        return True
    else:
        print(f"{Colors.RED}✗{Colors.END} {name}")
        return False

def main():
    print("\n" + "="*60)
    print(f"{Colors.BLUE}Air Quality Pipeline - Setup Verification{Colors.END}")
    print("="*60 + "\n")
    
    all_passed = True
    
    # 1. Check Python version
    print(f"\n{Colors.YELLOW}1. Python Environment{Colors.END}")
    import sys
    version = sys.version_info
    python_ok = version.major == 3 and version.minor >= 8
    check(f"Python version: {version.major}.{version.minor}.{version.micro}", python_ok)
    all_passed &= python_ok
    
    # 2. Check .env file
    print(f"\n{Colors.YELLOW}2. Configuration{Colors.END}")
    env_exists = os.path.exists('.env')
    check(".env file exists", env_exists)
    all_passed &= env_exists
    
    if env_exists:
        from dotenv import load_dotenv
        load_dotenv()
        token = os.getenv('AQICN_TOKEN')
        token_ok = token is not None and token != 'your_token_here'
        check("AQICN_TOKEN configured", token_ok)
        if not token_ok:
            print(f"  {Colors.YELLOW}→ Edit .env and add your AQICN token{Colors.END}")
            print(f"  {Colors.YELLOW}→ Get token from: https://aqicn.org/data-platform/token/{Colors.END}")
        all_passed &= token_ok
    
    # 3. Check Python packages
    print(f"\n{Colors.YELLOW}3. Python Dependencies{Colors.END}")
    packages = {
        'kafka': 'kafka-python',
        'pyspark': 'pyspark',
        'psycopg2': 'psycopg2-binary',
        'requests': 'requests',
        'dotenv': 'python-dotenv'
    }
    
    for module, package in packages.items():
        try:
            __import__(module)
            check(f"{package}", True)
        except ImportError:
            check(f"{package}", False)
            print(f"  {Colors.YELLOW}→ Run: pip install {package}{Colors.END}")
            all_passed = False
    
    # 4. Check Docker
    print(f"\n{Colors.YELLOW}4. Docker Infrastructure{Colors.END}")
    try:
        import subprocess
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True, timeout=5)
        docker_running = result.returncode == 0
        check("Docker is running", docker_running)
        
        if docker_running:
            containers = result.stdout.lower()
            kafka_ok = 'kafka' in containers
            timescale_ok = 'timescale' in containers
            minio_ok = 'minio' in containers
            
            check("Kafka container", kafka_ok)
            check("TimescaleDB container", timescale_ok)
            check("MinIO container", minio_ok)
            
            if not (kafka_ok and timescale_ok and minio_ok):
                print(f"  {Colors.YELLOW}→ Run: docker-compose up -d{Colors.END}")
                all_passed = False
        else:
            all_passed = False
            print(f"  {Colors.YELLOW}→ Start Docker Desktop{Colors.END}")
            print(f"  {Colors.YELLOW}→ Then run: docker-compose up -d{Colors.END}")
    except Exception as e:
        check("Docker check", False)
        print(f"  {Colors.YELLOW}→ Make sure Docker Desktop is installed and running{Colors.END}")
        all_passed = False
    
    # 5. Check project structure
    print(f"\n{Colors.YELLOW}5. Project Structure{Colors.END}")
    files = [
        'src/common/config.py',
        'src/ingestion/produce_city.py',
        'src/processing/spark_stream.py',
        'scripts/produce_hanoi.py',
        'scripts/run_spark.py',
        'docker-compose.yaml'
    ]
    
    for file in files:
        exists = os.path.exists(file)
        check(file, exists)
        all_passed &= exists
    
    # Summary
    print("\n" + "="*60)
    if all_passed:
        print(f"{Colors.GREEN}✓ All checks passed! Ready to run the pipeline.{Colors.END}")
        print("\n" + f"{Colors.BLUE}Next steps:{Colors.END}")
        print("1. Terminal 1: python scripts\\produce_hanoi.py")
        print("2. Terminal 2: python scripts\\run_spark.py")
        print("\nSee SETUP_GUIDE.md for detailed instructions.")
    else:
        print(f"{Colors.RED}✗ Some checks failed. Please fix the issues above.{Colors.END}")
        print("\nSee SETUP_GUIDE.md for setup instructions.")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
