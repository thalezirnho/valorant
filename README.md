# Valorant
 
Match Data: https://docs.henrikdev.xyz/valorant.html
Assets Data: https://valorant-api.com/

# Pipeline Structure

1. Layer Input
- input.py
Layer that collect data from api, store in google cloud stogare as `.json` files

2. Layer Tabular
- tabular.py
Layer that reads files inside storage, transform them into tables and ingest in Google Big Query

3. Layer Analytical
- analytical.py
Gold layer that create tables for end users and BI. 
Generate tables by reading `.sql` files inside `sql` folder using DDL commands