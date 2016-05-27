# Geppetto
by Datos IO
<http://www.datos.io>
==========

Introduction
------------
Geppetto is an automation and management framework for distributed systems. 

**Designed with Key Principles:**
* Modularity: Extendable. Easy to add functionality.
* Powerful: Includes functionality for installing, managing, and testing.
* Flexible: Full power and flexibility of Python.
* Easy: Abstracts all the heavy lifting so you can focus on getting the job done.

**What can be automated:**
###### Tests
* Automate 1 or 100's of tests. Test can include full setup, populations, management of a distributed system.
* Tests can be seconds long or months long.

###### Distributed Databases
* Cassandra
* Mongo
* Installation
* Population
* Fault injection
* Workload simulations
* Monitoring

Requirements
------------
##### System Libraries:
* SshPass
* Python 2.

##### Python Libraries: 
* Pymongo
* Boto
* Paramiko
* Pymongo
* Cassandra-Driver
* Awscli
* IPy
* Wget
* Scp

Installation
------------

And install script setup.sh is included for RHEL and Centos systems. Other platforms 
will have to manually install the required libraries by your method of choice. 
Either Brew on Mac OS X or apt-get on Debian/Ubuntu systems is recommended. 

To uninstall, drag Geppetto into the trash.

Usage
-----
**From the Geppetto home directory:**
```
python -m run -t tests/<test_file_path> -c configs/<config_file_path>
```

**Hello world example:**
```
python -m run -t tests/demos/hello_world.py -c configs/demos/sample_config.py
```

Enabling Email Settings
-----------------------
To enable emailing, update email configuration settings in common/common.py 
and add -e flag with email address.

**example:**
```
python -m run -t tests/demos/hello_world.py -c configs/demos/sample_config.py -e daenerys.targaryen@datos.io
```

Downloads
---------

Source code is available at <https://github.com/datosio/geppetto>.

License
-------

The Geppetto code is distributed under the MIT License. <https://opensource.org/licenses/MIT>


Version History
---------------

Version 1.0 - June 7, 2016

* Initial release.