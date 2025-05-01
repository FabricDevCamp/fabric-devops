"""AppLogger Module"""
import os
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
    def log_job_ended(cls, message=''):
        """log that job has ended"""
        print(' ', flush=True)
        print(f'> {message}', flush=True)
        print(' ', flush=True)

    @classmethod
    def log_step(cls, message):
        """log a step"""
        print(' ', flush=True)
        print('> ' + message, flush=True)

    @classmethod
    def log_substep(cls, message):
        """log a sub step"""    
        print('  - ' + message, flush=True)

    TABLE_WIDTH = 120

    @classmethod
    def log_table_header(cls, table_title):
        """Log Table Header"""
        print('', flush=True)
        print(f'> {table_title}', flush=True)
        print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)

    @classmethod
    def log_table_row(cls, column1_value, column2_value):
        """Log Table Row"""
        column1_width = 20
        column1_value_length = len(column1_value)
        column1_offset = column1_width - column1_value_length
        column2_width = cls.TABLE_WIDTH  - column1_width
        column2_value_length = len(column2_value)
        column2_offset = column2_width - column2_value_length - 5
        row = f'  | {column1_value}{" " * column1_offset}| {column2_value}{" " * column2_offset}|'
        print(row, flush=True)
        print('  ' + ('-' * (cls.TABLE_WIDTH)), flush=True)


    @classmethod
    def log_error(cls, message):
        """log error"""
        error_message = "ERROR: " + message
        print('-' * len(error_message), flush=True)
        print(error_message, flush=True)
        print('-' * len(error_message), flush=True)
