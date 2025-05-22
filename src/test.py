"""Deploy Power BI Solution"""

import os

vars = os.environ.items()

print("Environmental Variables")
for key, value in vars:
    print(f' - {key}: {value}')