from setuptools import setup, find_packages

setup(
    name="data_automation_5",
    version="2.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas", "requests", "sqlalchemy", "psycopg2-binary", "openpyxl"
    ],
    entry_points={
        'console_scripts': [
            'data-auto-gui=scripts.run_gui:main',
            'data-auto-cli=scripts.run_analysis:main',
        ],
    },
)