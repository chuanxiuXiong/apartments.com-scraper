# A scraper retrieving apartment data from apartments.com


## Table of Contents
- [Introduction](#introduction)
- [Getting Started](#getting-started)


## Introduction

This tool scrapes selected data from apartments.com platform (https://www.apartments.com/) into the mysql database. It loads the zipcode list from the database, search for fitness centers within the zipcode range and stores the data back to the database. Provided codes also supports tranferring the credentials of database connection from aws s3 bucket and dockerizing the application.


<img src="https://cdn4.iconfinder.com/data/icons/logos-and-brands/512/267_Python_logo-512.png" height="75" style="margin: 10px; float: left;">
<img src="https://img.icons8.com/color/48/000000/docker.png" height="75" style="margin: 10px; float: left;">
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/AmazonWebservices_Logo.svg/580px-AmazonWebservices_Logo.svg.png" height="75" style="margin: 10px; float:left;">
<div style="clear:both;"></div>

## Getting Started
Please follow these steps before running the code for the first time.
1. Install [Python 3](https://www.python.org/downloads/)
2. Install all the packages listed in packages.txt. If you have [pip](https://pypi.org/project/pip/) installed, you could run `pip install -r packages.txt` to install all the packages.
3. Replace the function content of `create_database_engine` so that it matches your database connection info.
4. Run the program by `python3 main.py`.


