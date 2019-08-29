djfirebirdsql
==============

Django firebird https://firebirdsql.org/ database backend.

I referrerd django-firebird database backend https://github.com/maxirobaina/django-firebird .

Requirements
-------------

* Django 2.1+
* Firebird 4.0+
* pyfirebirdsql (https://github.com/nakagami/pyfirebirdsql) recently released.

Installation
--------------

::

    $ pip install firebirdsql djfirebirdsql django

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
