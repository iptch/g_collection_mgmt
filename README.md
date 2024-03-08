# G-Collection Management
Management and util functionality for the g-collection game.

## Load user info from Google Sheet
1. Download Gsheet as CSV [here](https://docs.google.com/spreadsheets/d/1Ct1uE8G4pQDmQRw8pa_UOiwE1khM3TKhpLeO0GWzcSs/edit?usp=sharing)
2. Download List of all acronyms as CSV [here](https://docs.google.com/spreadsheets/d/1zk8QXxjcUVOc_ah_QYgxYSRjUb9DKpuownFEyFipgeQ/edit?usp=sharing)
3. Copy both CSV to `user_info_db_load/data`
4. run `python user_info_db_load/main.py POSTGRES_PW="XXX"`

## Create images with <email>.jpg name based on <acronym>.jpg
--> image_copy/main.py

## Load Card Images
--> Not implemented yet

## Create Thumbnails of Card images
--> Not implemented yet
