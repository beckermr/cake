from setuptools import setup, find_packages

setup(
    name='cake',
    version='0.2',
    description="featherweight task management for high throughput computing",
    author='Matthew R. Becker',
    author_email='becker.mr@gmail.com',
    license="BSD 3-clause",
    url='https://github.com/beckermr/cake',
    packages=find_packages(),
    entry_points={'console_scripts': ['cake=cake.__main__:cli']},
    install_requires=['pyyaml', 'click', 'numpy'])
