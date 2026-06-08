import os
import sys
import tempfile
import argparse
import subprocess
from IPython.core.magic import register_cell_magic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tmp_python_file(cell):
    # Create a temporary file with the cell contents
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
        temp_file.write(cell)
        temp_file_path = temp_file.name
    return temp_file_path

def parse_arguments_pyflink(line):
    parser = argparse.ArgumentParser(description="Submit PyFlink job")
    parser.add_argument("--jobmanager", required=True, help="JobManager address")
    parser.add_argument("-pyarch", help="Path to virtual environment archive")
    parser.add_argument("-pyexec", help="Python executable path within the archive")
    parser.add_argument("-p", "--parallelism", type=int, help="Parallelism")
    parser.add_argument("-c", "--class", dest="class_name", help="Class with the program entry point")
    parser.add_argument("-jar", help="Path to JAR file")
    parser.add_argument("--pyFiles", help="Python files to be added to the PYTHONPATH")
    parser.add_argument("--target")
    parser.add_argument("additional_args", nargs=argparse.REMAINDER, help="Additional arguments to pass to Flink")
    return parser.parse_args(line.split())

@register_cell_magic
def submit_to_cli(line, cell):
    temp_file_path = create_tmp_python_file(cell)
    os.system(f'{sys.executable} {temp_file_path} {line}')
    os.remove(temp_file_path)

@register_cell_magic
def submit_pyflink(line, cell):
    args = parse_arguments_pyflink(line)

    # Create a temporary file with the cell contents
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
        temp_file.write(cell)
        temp_file_path = temp_file.name

    # Construct the flink run command
    flink_root = os.environ.get("FLINK_ROOT_DIR")
    if not flink_root:
        raise EnvironmentError("FLINK_ROOT_DIR environment variable not set.")
    flink = f"{flink_root}/bin/flink"

    flink_command = [flink, "run"]
    flink_command.extend([f"--jobmanager", args.jobmanager])

    if args.pyarch:
        flink_command.extend(["-pyarch", args.pyarch])
    if args.pyexec:
        flink_command.extend(["-pyexec", args.pyexec])
    if args.parallelism:
        flink_command.extend(["-p", str(args.parallelism)])
    if args.class_name:
        flink_command.extend(["-c", args.class_name])
    if args.jar:
        flink_command.extend(["-jar", args.jar])
    if args.pyFiles:
        flink_command.extend(["--pyFiles", args.pyFiles])
    if args.target:
        flink_command.extend(["--target", args.target])

    flink_command.extend(args.additional_args)

    flink_command.append(f"-py")
    flink_command.append(temp_file_path)

    # Execute the command
    command_str = " ".join(flink_command)
    logger.info(f"Executing: {command_str}")

    # Execute the command and capture stdout and stderr
    process = subprocess.Popen(command_str, env=os.environ, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    logger.info(output.decode())
    logger.Error(error.decode())

    #result = subprocess.run(command_str, capture_output=True, text=True, shell=True)
    #logger.info(result.stdout)
    #logger.error(result.stderr)



    # Clean up the temporary file
    os.remove(temp_file_path)

def load_ipython_extension(ipython):
    ipython.register_magic_function(submit_to_cli, 'cell')
    ipython.register_magic_function(submit_pyflink, 'cell')
