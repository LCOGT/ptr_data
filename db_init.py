import os
from os.path import join, dirname
from configparser import ConfigParser

DB_CONFIG_FILE = os.path.dirname(__file__) + '/database.ini'

def config(filename=DB_CONFIG_FILE):
    # create parser
    parser = ConfigParser()

    # read the configuration
    parser.read(filename)
    
    # identify sections
    sections = parser.sections()

    # get the section values
    params = {}
    for section in sections:
        section_values = get_section(parser, section)
        params[section] = section_values
        print(section)
        print(section_values)
    return params

def get_section(parser, section):
    values = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            values[param[0]] = param[1]
    else:
        raise Exception('Section %s not found' % section)
    
    return values