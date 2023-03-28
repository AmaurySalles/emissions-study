# emissions-study
Data analysis of GHG emissions in France, from various 3rd party sources

Clean data can be viewed on a streamlit app:

![image](https://user-images.githubusercontent.com/90214548/228274878-a9b06596-4232-462b-a1d4-1ab818356478.png)


## Install

1. Clone repo
2. Within repo, create new virtual environment by running the following: `python3 -m venv .venv`
3. Activate virtual environment with `source .venv/bin/activate`
4. Install all the requirements `python3 -m pip install -r requirements.txt`

## Run

To initialise the database, run the `emissions.py` script with the following command: `python3 -m emissions.py`

To launch the streamlit app: `streamlit run app.py`

## DB-Schema

DB-Schema.xml can be found under `./doc`

![image](https://user-images.githubusercontent.com/90214548/228275276-8b5e41d6-790b-4071-b0cb-3111f73fecd8.png)
