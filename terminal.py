# -*- coding: utf-8 -*-
"""
Created on Sat May  7 12:05:17 2022

@author: aphorikles
"""

from datetime import datetime as dt



class terminal:
    def read(file):
        with open(file, 'r') as f:
            contents = f.read()
        
        return contents
            
    def ask_name(name_for):
        name = input(f'\nenter {name_for}: ')
        
        return name
    
    def ask_yes_no(msg):
        answer = input(f'\n{msg} (y/n): ')
        answer = True if answer.lower() == 'y' else False
        
        return answer
        
    def ask_created():
        to_dt = lambda o, f: dt.strptime(o, f)
        dt_format = '%Y-%m-%d-%H-%M'
        now = dt.now().strftime(dt_format)
        prompt_dt_format = 'yyyy-mm-dd-hh-mm'
        prompt = f'enter card creation date ({prompt_dt_format})'
        prompt = f'\n{prompt}\nor press enter for {now}: '
        created = input(prompt)
        
        try:
            created = to_dt(created, dt_format) if created else to_dt(now, dt_format)
        except:
            print('error: please enter date in format yyyy-mm-dd-hh-mm')
            return terminal.ask_created()
        
        return created



if __name__ == '__main__':
    terminal.print_file_contents('data/indexia_lower.txt')
