import subprocess

def execute(command, inp=None):
    com = command.split()
    out = subprocess.run(com,capture_output=True, input = inp, text=True)
    if "create" in com:
        return out.returncode
    else:
        return out.stdout

def fio(cmd):

    out = subprocess.check_output(cmd,shell = True, text = True)
    return out

