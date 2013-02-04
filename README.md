# APTrust Message Queue Library

This is only prototype and testing code while we work on policies
for the DPN messaging queue.

## Overview

This codebase deals with message routing related to the Academic
Preservation Trust.

**Currently** this only provides a simple logging consumer to 
test and record messages from specific queues as well as a
command line producer for convienence in sending messages.

## Running on Virtualenv

This code can be run like any other python library but we recommend using
virtualenv and virtualenvwrapper as a best practice to avoid conflicts
and allow for a sustainable environment.  These libraries are **NOT** 
installed as part of the pip requirements file as you are free to
choose your environmental setup yourself.

For more information see:

* Virtualenv: http://pypi.python.org/pypi/virtualenv
* Virtualenvwrapper: http://virtualenvwrapper.readthedocs.org/en/latest/

## Dependencies

Dependencies for this project are listed in the pip requirements file
`requirements.txt` located under the root of this project.

To setup using pip just:

1.  Activate the appropriate Virtualenv if using that setup.
2.  From the project root execute `pip install -r requirements`

## Use

To setup a logging consumer just activate the appropriate environment if using
virtual env and type:

     >python consumer.py`

if you wish to let this run in the background while you're logged out just use
screen as in:

    >screen python consumer.py