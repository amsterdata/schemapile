#!/usr/bin/env python
# coding: utf-8

import os
import json
from pathlib import Path

from file_handler import FileHandler
from pglast_parse import PGLast

# In[33]:


sqlfiles_dir = "data/sqlfiles_permissive/"
output = "output/pglast/"
files = os.listdir(sqlfiles_dir)

parsed_statements = 0

for i, file in enumerate(files):
    outpath = output+file+".result"
    if Path(outpath).exists():
        continue

    handler = FileHandler(sqlfiles_dir+file)
    handler.get_file_details()
    encoding = handler.get_data()[1]["encoding"]
    try:
        glot = PGLast(sqlfiles_dir+file, encoding)
        glot.parse_file()
        with open(outpath, "w+") as f:
            parsed_statements += sum([res['parsed'] for res in glot.results])
            json.dump(glot.get_flat_data(), f)
    except:
        continue
    print(f'{i}/{len(files)} - parsed statements: {parsed_statements}')


# In[ ]:


print(parsed_statements)

