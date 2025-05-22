"""Test 2"""

import os
import atexit
import msal

def test1():
    """Test1"""
    cache_folder = ".//.cache//"
    cache_file_name = "token-cache.bin"
    cache_file_path = os.path.join(cache_folder, cache_file_name)

    if not os.path.exists(cache_folder):
            os.makedirs(cache_folder)

    with open(cache_file_path, 'w', encoding='utf-8') as file:
        file.write('hello')


def create_cache():
    """Create Token Cache"""
    cache_folder = ".//.cache"
    cache_file_name = "token-cache.bin"
    cache_file_path = os.path.join(cache_folder, cache_file_name)

    cache = msal.SerializableTokenCache()
    
    if os.path.exists(cache_file_path):
        cache.deserialize(open(cache_file_path, "r", encoding="utf-8").read())
    
    atexit.register(lambda:
        open(cache_file_path, "w", encoding='utf-8').write(cache.serialize())
        # Hint: The following optional line persists only when state changed
        if cache.has_state_changed else None
        )
    

test1()
