import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

requires = [
    'sprout_orm',
    'bud_orm',
    'requests',
    'mpmath'
    ]

tests_require = [
]

setup(name='schema_conversion',
      version='0.0',
      description='SchemaConversion',
      long_description=README + '\n\n' + CHANGES,
      author='Kelley Paskov',
      author_email='kpaskov@stanford.edu',
      url='',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points="""\
      [paste.app_factory]
      main = schema_conversion.convert_all:main
      """,
      )
