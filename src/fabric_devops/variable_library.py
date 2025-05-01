""" Variable Library"""
import json
from typing import List

class Variable:
    """Variable in Variable Library"""
    name: str
    value: str
    type: str
    note: str

    def __init__(self, name: str, value: str, variable_type: str = 'String', note: str = '') -> None:
        self.name = name
        self.value = value
        self.type = variable_type
        self.note = note

    def encode(self):
        """encode support"""
        return self.__dict__

class VariableLibrary:
    """Variable Library"""
    variables: List[Variable]

    def __init__(self):
        self.variables = []

    def encode(self):
        """encode support"""
        return self.__dict__

    def add_variable(self, name: str, value: str, variable_type: str = 'String', note: str = 'some note'):
        """"add variable"""
        self.variables.append( Variable(name, value, variable_type, note) )

    def get_variable_json(self):
        "Get JSON for variables.json"
        variable_lib = {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json',
            'variables': self.variables
        }
        return json.dumps(variable_lib, default=lambda o: o.encode(), indent=4)

