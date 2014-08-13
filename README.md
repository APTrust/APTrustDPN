# DPN Node Project

This is a very early prototype for an application to manage
functions needed to operate as a Node in the DPN project.

For more information about DPN, see it's website at: http://www.dpn.org/


## Overview

This project uses the Django Framework to wrap functions for a
message exchange, celery worker processes and registry
functionality.  It primarily uses Django and Django-Celery for
task management and registry interaction.  Look up those
projects for more information.

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
2.  From the project root execute `pip install -r requirements.txt`

To run unittests you will also need to `pip install -r requirements_dev.txt`

### Dependencies for testing

Some additional dependencies are required for unit tests to run.  (mock
libraries and such).  If running tests be sure to also pip install from
requirements_dev.txt in addition to the normal requirements.

## Use

More to come.

