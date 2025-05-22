"""Deploy Power BI Solution"""

import os

_token_cache_folder = './/Cash//'
_token_cache_file = 'token-cache.bin'
_token_cache = None


if not os.path.exists(_token_cache_folder):
        os.makedirs(_token_cache_folder)

cache_file_path = os.path.join(
    _token_cache_folder,
    _token_cache_file)

cache_file = open(cache_file_path, 'w', encoding='utf-8')
cache_file.write('hey baby')

# cls._token_cache.serialize()