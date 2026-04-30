import importlib._bootstrap_external as be
import pathlib
import sys
import time
source_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
virtual_name = sys.argv[3]
output_path.parent.mkdir(parents=True, exist_ok=True)
source_bytes = source_path.read_bytes()
code = compile(source_bytes, virtual_name, 'exec')
data = be._code_to_timestamp_pyc(code, int(time.time()), len(source_bytes))
output_path.write_bytes(data)