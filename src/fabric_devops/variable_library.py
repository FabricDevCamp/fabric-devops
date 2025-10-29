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

class VariableOverride:
    """Variable in Variable Library"""
    name: str
    value: str

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value

    def encode(self):
        """encode support"""
        return self.__dict__

class Valueset:
    """Variable Library"""
    variableOverrides: List[VariableOverride]

    def __init__(self, name):
        self.name = name
        self.variableOverrides = []

    def encode(self):
        """encode support"""
        return self.__dict__
    
    def add_variable_override(self, name: str, value: str):
        """"add variable"""
        self.variableOverrides.append( VariableOverride(name, value) )

class VariableLibrary:
    """Variable Library"""
    variables: List[Variable]
    valuesets: List[Valueset]

    def __init__(self, variables = None, valuesets = None):
        self.variables = variables if variables is not None else []
        self.valuesets = valuesets if valuesets is not None else []
        self.valueSetsOrder = []

    def encode(self):
        """encode support"""
        return self.__dict__

    def add_variable(self, name: str, value: str, variable_type: str = 'String', note: str = 'some note'):
        """"add variable"""
        self.variables.append( Variable(name, value, variable_type, note) )

    def add_valueset(self, valueset: Valueset):
        """"add valueset"""
        self.valuesets.append( valueset )
        self.valueSetsOrder.append(valueset.name)

    def get_variable_json(self):
        "Get JSON for variables.json"
        variable_lib = {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json',
            'variables': self.variables
        }
        return json.dumps(variable_lib, default=lambda o: o.encode(), indent=4)

    def get_valueset_json(self, valueset_name):
        """Get JSON for valueset.json"""
        valueset_list = list(filter(lambda valueset: valueset.name == valueset_name, self.valuesets))
        valueset: Valueset = valueset_list[0]
               
        valueset_export = {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json',
            'name': valueset.name,
            'variableOverrides': valueset.variableOverrides
        }
        return json.dumps(valueset_export, default=lambda o: o.encode(), indent=4)
