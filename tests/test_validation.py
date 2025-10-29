"""
Basic validation tests for the image_db_tool package.

These tests verify module structure, imports, and basic functionality
without requiring a database or actual archive data.
"""

import sys
import os
from pathlib import Path

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


def test_imports():
    """Test that all modules can be imported."""
    print("Testing module imports...")
    
    try:
        import image_db_tool
        print("✓ image_db_tool package imported")
        
        from image_db_tool import config
        print("✓ config module imported")
        
        from image_db_tool import database
        print("✓ database module imported")
        
        from image_db_tool import scanner
        print("✓ scanner module imported")
        
        from image_db_tool import processor
        print("✓ processor module imported")
        
        from image_db_tool import orchestrator
        print("✓ orchestrator module imported")
        
        from image_db_tool import cli
        print("✓ cli module imported")
        
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False


def test_config_creation():
    """Test configuration object creation without file."""
    print("\nTesting configuration creation...")
    
    try:
        from image_db_tool.config import Config
        
        # Create config without file (should use defaults/env vars)
        config = Config(config_path='/nonexistent/config.yaml')
        print("✓ Config object created without file")
        
        # Test get method
        value = config.get('database', 'host', 'default_host')
        print(f"✓ Config.get() works (returned: {value})")
        
        return True
    except Exception as e:
        print(f"✗ Config test failed: {e}")
        return False


def test_scanner_object_id_end():
    """Test object ID end calculation."""
    print("\nTesting scanner object_id_end calculation...")
    
    try:
        from image_db_tool.scanner import ArchiveScanner
        
        # Test cases
        test_cases = [
            ('W22084', '84'),
            ('W00001', '01'),
            ('W1234A', '00'),  # Not digits
            ('WXY', '00'),     # Less than 2 chars
            ('W00000', '00'),
        ]
        
        for object_id, expected in test_cases:
            result = ArchiveScanner.calculate_object_id_end(object_id)
            if result == expected:
                print(f"✓ {object_id} -> {result}")
            else:
                print(f"✗ {object_id} -> {result} (expected {expected})")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Scanner test failed: {e}")
        return False


def test_processor_sha256():
    """Test SHA256 hash calculation."""
    print("\nTesting processor SHA256 calculation...")
    
    try:
        from image_db_tool.processor import ImageProcessor
        import tempfile
        
        processor = ImageProcessor()
        
        # Create a temporary file with known content
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            temp_path = f.name
            f.write("test content\n")
        
        try:
            # Calculate hash
            hash_bytes = processor.calculate_sha256(temp_path)
            
            # Verify it's 32 bytes
            if len(hash_bytes) == 32:
                print(f"✓ SHA256 hash calculated (32 bytes)")
                print(f"  Hash: {hash_bytes.hex()[:16]}...")
                return True
            else:
                print(f"✗ Hash is {len(hash_bytes)} bytes, expected 32")
                return False
        finally:
            os.unlink(temp_path)
    
    except Exception as e:
        print(f"✗ Processor test failed: {e}")
        return False


def test_database_input_validation():
    """Test database manager input validation."""
    print("\nTesting database input validation...")
    
    try:
        from image_db_tool.database import DatabaseManager
        
        # We can't test actual database operations without a DB,
        # but we can test that the class exists and has expected methods
        methods = [
            'get_or_create_root',
            'get_or_create_object',
            'get_or_create_file',
            'add_file_path',
            'add_image_info',
            'batch_insert_paths',
        ]
        
        for method in methods:
            if hasattr(DatabaseManager, method):
                print(f"✓ DatabaseManager.{method} exists")
            else:
                print(f"✗ DatabaseManager.{method} missing")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Database validation test failed: {e}")
        return False


def test_orchestrator_structure():
    """Test orchestrator class structure."""
    print("\nTesting orchestrator structure...")
    
    try:
        from image_db_tool.orchestrator import ImageDatabaseOrchestrator
        
        # Check for expected methods
        methods = [
            'process_object',
            'process_all',
        ]
        
        for method in methods:
            if hasattr(ImageDatabaseOrchestrator, method):
                print(f"✓ ImageDatabaseOrchestrator.{method} exists")
            else:
                print(f"✗ ImageDatabaseOrchestrator.{method} missing")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Orchestrator test failed: {e}")
        return False


def run_all_tests():
    """Run all validation tests."""
    print("="*60)
    print("Running Image DB Tool Validation Tests")
    print("="*60)
    
    tests = [
        test_imports,
        test_config_creation,
        test_scanner_object_id_end,
        test_processor_sha256,
        test_database_input_validation,
        test_orchestrator_structure,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"✗ Test {test.__name__} raised exception: {e}")
            results.append(False)
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1


if __name__ == '__main__':
    exit_code = run_all_tests()
    sys.exit(exit_code)
