djfirebirdsql
==============

Django firebird https://firebirdsql.org/ database backend.

I referrerd django-firebird database backend https://github.com/maxirobaina/django-firebird .

Requirements
-------------

* Django 3.1
* Firebird 4.0 beta1 (Firebird 3.0 minimum version required)
* pyfirebirdsql (https://github.com/nakagami/pyfirebirdsql) recently released.

Installation
--------------

::

    $ pip install firebirdsql djfirebirdsql django==3.1

Database settings example
------------------------------

::

    DATABASES = {
        'default': {
            'ENGINE': 'djfirebirdsql',
            'NAME': '/path/to/database.fdb',
            'HOST': ...,
            'USER': ...,
            'PASSWORD': ...,
        }
    }
