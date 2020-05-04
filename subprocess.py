import subprocess

def executeCommand(
    command_words,
    is_silent = False
):
    process = subprocess.Popen(command_words, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout_stream, stderr_stream = process.communicate()
    stdout = stdout_stream.decode('utf-8')
    stderr = stderr_stream.decode('utf-8')

    if is_silent:
        if process.returncode != 0:
            raise Exception('\n'.join([
                'A non-zero return code has been encountered:',
                'command: ' + ' '.join(command_words),
                f'return_code: {process.returncode}',
                f'stdout: {stdout}',
                f'stderr: {stderr}',
            ]))
        
        return {
            'return_code': process.returncode,
            'stdout': stdout,
            'stderr': stderr
        }
    else:
        print('== COMMAND ==')
        print(' '.join(command_words))
        print('== RESULT ==')
        print(process.returncode)
        print('== OUT ==')
        print(stdout)
        print('== ERR ==')
        print(stderr)
