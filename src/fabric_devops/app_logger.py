"""AppLogger Module"""

class AppLogger:
    """Logic to write log output to console and/or logs"""

    @classmethod
    def log_job(cls, message):
        """start job"""
        print('', flush=True)
        print('|' + ('-' * (len(message) + 2)) + '|', flush=True)
        print(f'| {message} |', flush=True)
        print('|' + ('-' * (len(message) + 2)) + '|', flush=True)

    @classmethod
    def log_step(cls, message):
        """log a step"""
        print(' ', flush=True)
        print('> ' + message, flush=True)

    @classmethod
    def log_substep(cls, message):
        """log a sub step"""    
        print('  - ' + message, flush=True)

    @classmethod
    def log_error(cls, message):
        """log error"""
        error_message = "ERROR: " + message
        print('-' * len(error_message), flush=True)
        print(error_message, flush=True)
        print('-' * len(error_message), flush=True)

    @classmethod
    def log_job_ended(cls, message=''):
        """log that job has ended"""
        print(' ', flush=True)
        print(f'> {message}', flush=True)
        print(' ', flush=True)
